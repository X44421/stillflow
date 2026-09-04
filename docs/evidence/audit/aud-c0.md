# AUD-C0 — Audit event contract

Issue: #261 — `[AUD-C0] Audit event contract`
Predecessor: OPS-O3, merged at `bb23334aaac35b37a4be910d49ff197ba6ffc3c7`
Scope: contract and evidence only; no runtime, schema, transport, retention
job, export implementation, or UI.

## Contract purpose

This document freezes the audit vocabulary and invariants consumed by AUD-A1,
retention/GC, automation, authorization, and H2/H3. It is provider- and
transport-neutral. The existing control-plane `cp_events` stream remains the
authority for Job/Run lifecycle delivery; `AuditEvent` is a separate durable
record family for security, governance, provenance, and operator accountability.

An implementation may project a control-plane event into an audit record when
the action is auditable, but the projection must reference the source event and
must not create a second lifecycle sequence or claim that a system event is an
audit event. A failed projection cannot rewrite the source lifecycle event.

## Versioned v1 envelope

Every audit record is a canonical, versioned envelope. Field names below are
logical contract names; the storage and transport adapters may use their own
serialization casing while preserving the meaning and required fields.

| Field | Type / bound | Contract rule |
| --- | --- | --- |
| `auditEventId` | non-nil UUID | Globally unique event identity; never reused after rejection or retention |
| `auditVersion` | positive integer, currently `1` | Unknown or future versions fail closed; compatible readers must select an explicit version |
| `workspaceId` | non-nil UUID | Security/tenant boundary; every record belongs to exactly one workspace |
| `sequence` | positive u64, assigned by the audit writer | Monotonic within one workspace; authoritative ordering key, never supplied by a caller |
| `occurredAt` | UTC RFC 3339 timestamp | Describes when the action occurred; it does not replace `sequence` and cannot move an event backwards |
| `actor` | typed `AuditActor` | Identifies the authenticated human, service account, or system actor without secret material |
| `action` | bounded identifier | Stable machine-readable verb such as `workspace.member.added`, `job.cancelled`, or `artifact.exported` |
| `reasonCode` | bounded identifier | Required for policy/security actions; explains why without requiring raw user or cell content |
| `requestId` | non-empty bounded identifier | Correlates the originating request; secret markers and arbitrary untrusted headers are forbidden |
| `correlationId` | non-empty bounded identifier | Groups causally related actions across adapters and retries |
| `traceId` | optional bounded identifier | Diagnostic link only; absence is valid and does not change authorization or identity |
| `object` | typed `AuditObjectRef` | Primary affected workspace object; object identity is a reference, not a disclosure grant |
| `before` / `after` | optional `AuditStateRef` | Digest/reference to a state version or event, never a raw state dump |
| `lineage` | bounded list of typed edges | Links related Dataset, PlanVersion, Run, and Artifact identities in the same workspace |
| `sourceEventId` | optional UUID | References one existing control-plane/system event when the audit record is a projection; it is not a replacement sequence |
| `payload` | typed, redaction-safe object, max 64 KiB encoded | Contains only bounded policy metadata; unknown payload fields fail closed for strict consumers |
| `idempotencyKey` | optional bounded identifier | Scoped to `(workspaceId, producer, idempotencyKey)`; same digest replays the original result, a different digest is rejected |

The canonical digest for idempotency and export is computed over the canonical
v1 envelope with `sequence` and storage timestamps excluded from the caller
preimage. The assigned sequence remains part of the persisted record and its
ordered export representation. Canonicalization must sort object keys and
lineage edges and use one stable encoding for timestamps and UUIDs.

## Actor model

`AuditActor` is a tagged value with exactly one of these modes:

| Mode | Required identity | Semantics |
| --- | --- | --- |
| `user` | workspace-scoped member/principal reference | Human action after authentication and authorization resolution |
| `serviceAccount` | owning-workspace service-account reference | Non-human automation with explicit capability and credential lifecycle |
| `system` | stable system actor code | Internal recovery, migration, retention, reconciliation, or control-plane action; never an anonymous fallback for a caller |

The actor includes a stable opaque `actorRef` and a bounded `actorKind`. A
display name, email, token subject, authorization header, credential reference
value, or provider response may not be used as a substitute for the opaque
identity. The authenticated principal and authorization decision are resolved
before append; `actor=system` must be selected explicitly by a trusted backend
operation and cannot be requested by a client to bypass actor accountability.

## Action and reason vocabulary

`action` is a versioned identifier, not a free-form sentence. v1 action families
are:

- `workspace.*`, `member.*`, `role.*`, `service_account.*`, and `credential.*`
  for tenancy and identity lifecycle;
- `dataset.*`, `snapshot.*`, `plan.*`, and `plan_version.*` for durable data and
  plan governance;
- `job.*`, `run.*`, `artifact.*`, and `export.*` for execution and outputs;
- `audit.read`, `audit.export`, `audit.retained`, `audit.expired`, and
  `audit.redacted` for audit operations themselves;
- `system.recovered`, `system.migrated`, and `system.reconciled` for trusted
  backend maintenance.

Each action declares whether an object, a reason code, a before/after reference,
or a source control-plane event is required. A later action must not silently
change the meaning of an existing action; a new semantic requires a new action
identifier or envelope version.

`reasonCode` is a bounded code such as `user_request`, `policy_enforcement`,
`retry_replay`, `retention_cutoff`, `recovery`, or `migration`. Free-form
explanation text is optional, bounded, sanitized, and must never carry raw cell
values, credentials, authorization claims, or provider payloads.

## Object references and lineage

`AuditObjectRef` contains `objectKind`, `objectId`, and `workspaceId`. The
workspace must equal the envelope workspace. Object IDs are opaque; knowing an
ID is never proof that the caller may read the object or its audit history.

The v1 object-kind vocabulary includes `Workspace`, `Member`, `Role`,
`ServiceAccount`, `CredentialRef`, `Dataset`, `Snapshot`, `Plan`, `PlanVersion`,
`Job`, `Run`, `Artifact`, `VerificationBundle`, `Export`, and `AuditEvent`.
Unknown kinds fail closed until a compatible contract revision adds them.

Lineage edges are typed references with `from`, `to`, and `edgeKind`. The v1
governed chain is:

| Edge | Meaning |
| --- | --- |
| `Dataset -> PlanVersion` | Plan version consumes or is defined for the dataset |
| `PlanVersion -> Run` | Run executes the immutable plan version |
| `Run -> Artifact` | Run produces or commits the artifact |
| `Snapshot -> PlanVersion` | Plan version consumes an immutable snapshot input |
| `Artifact -> Export` | Export is derived from the committed artifact |

All edges are workspace-scoped, bounded, sorted canonically, and references
only. A lineage graph may be incomplete when an upstream object is unavailable,
but it may not invent an edge or use an unverified cross-workspace reference.
The lineage list is not a second source of execution truth; the authoritative
Dataset, PlanVersion, Run, Artifact, Snapshot, and Export records remain owned
by their existing domain/storage authorities.

## Before/after and payload safety

`AuditStateRef` is one of:

- an opaque source event ID;
- a durable object/version ID plus a canonical digest;
- a typed state marker such as `active`, `revoked`, `committed`, `tombstoned`,
  or `failed` where the marker is already part of the public contract.

It is not a JSON copy of a Dataset row, plan body, credential, cell, token, or
artifact body. If a before/after value is too large or sensitive, the record
stores only its type, version, and digest-safe reference. The payload may carry
bounded counts, state markers, policy codes, object references, and digest
metadata. It may not carry raw cell values, secret material, bearer tokens,
authorization headers, provider response bodies, private keys, or reversible
encodings.

The same safe-JSON rules used by the backend apply to audit metadata. Payload
size, string lengths, collection cardinalities, nesting depth, and total
lineage edges are bounded before any visible append. Validation failure creates
no partial record and does not echo the rejected sensitive value.

## Immutability, ordering, and idempotency

1. Append is the only normal mutation. There is no update-by-ID or
   delete-by-path audit API.
2. The writer assigns one workspace-local sequence in the commit transaction.
   Concurrent writers serialize on that authority; caller timestamps are not
   ordering authority.
3. `auditEventId` is unique. A retry with the same idempotency tuple and the
   same canonical digest returns the already committed identity; the same tuple
   with a different digest fails closed without changing the original record.
4. A rejected append consumes no visible sequence and publishes no partial
   payload. A committed event is never rewritten to repair malformed data.
5. Reads and exports use `(workspaceId, sequence, auditEventId)` as a stable
   deterministic order. They must not sort by caller-controlled display text.
6. Control-plane event sequence remains per Job/Run stream as defined by
   `cp_events`; it is never reused as the audit workspace sequence.

## Control-plane events versus audit events

The current control-plane record carries lifecycle fields including
`event_id`, `stream_kind`, `stream_id`, `sequence`, `event_type`, `job_id`,
`run_id`, `request_id`, `correlation_id`, `actor_ref`, and a bounded payload.
Those records serve Job/Run lifecycle replay and SSE-style delivery.

Audit records use a distinct identity, workspace sequence, action vocabulary,
object/lineage references, and retention/export rules. The mapping rules are:

- Job/Run lifecycle transitions remain authoritative in `cp_events` and are
  not duplicated into a second lifecycle stream.
- An auditable projection may carry `sourceEventId` and the same redaction-safe
  request/correlation references, but it receives its own audit identity and
  workspace sequence.
- Audit reads must not expose a control-plane event merely because the caller
  can query audit records; the later API must choose an explicit projection or
  source-event view with the corresponding authorization.
- A control-plane event may exist without an audit projection, and an audit
  record may describe a governance action that has no Job/Run event.
- Neither family can be silently used to infer the other family's retention,
  pagination cursor, or authorization semantics.

## Authorization, hidden resources, and cursor isolation

Audit access is workspace-scoped and consumes SEC-A1 authorization. The minimum
capabilities are `audit.read` for bounded queries and `audit.export` for an
explicit export operation. Actor/object filters narrow an already authorized
scope; they cannot grant access to another workspace or reveal whether a hidden
object exists.

Every query validates:

- the authenticated principal and workspace membership/service-account scope;
- the requested action/object/actor filters and their bounded cardinality;
- a maximum page size and maximum time span/scan budget;
- an optional cursor whose workspace, filter digest, audit version, and last
  sequence exactly match the request.

Cross-workspace cursors, foreign object references, malformed filters, future
versions, and hidden resources fail closed with the same not-found or typed
request semantics required by SEC-C0. Totals and timing must not become an
existence oracle. Query results contain only fields allowed by redaction policy;
authorization is enforced before any object or actor metadata is disclosed.

## Retention and tombstone behavior

Audit records are immutable during their retention period. Retention is a
policy decision, not an implicit side effect of ordinary reads, exports, or
control-plane GC. A retention worker may make a record ineligible for ordinary
query after a workspace policy cutoff, but it must not mutate the record in
place.

Physical archival or deletion, if later authorized by the retention policy,
must be:

- bounded by workspace, cutoff, and candidate count;
- blocked by legal/operational holds and unresolved lineage references;
- performed through a maintenance gate with crash-safe staging/recovery;
- represented by a new redaction-safe `audit.expired` or `audit.retained`
  event in the surviving audit history;
- idempotent and never addressable through an arbitrary filesystem path.

An expired or tombstoned event is not returned as a normal full record. A
policy-approved metadata marker may remain, but raw payload and sensitive
references are not reconstructed. Retention must not delete Dataset, Snapshot,
Plan, Run, Artifact, or Export records merely because an audit record expired;
those domains retain their own reference-safe policies.

## Audit export policy

`audit.export` is an explicit, authorized, bounded read that produces a
deterministic ordered representation and a manifest/digest for the selected
workspace, filters, time range, and audit version. Export:

- includes only committed, non-hidden, redaction-safe audit fields;
- preserves workspace sequence and canonical ordering;
- records the filter/policy/version preimage and result digest;
- refuses unbounded ranges, foreign cursors, symlinked or pre-existing
  destinations, and destination overwrite;
- does not copy credentials, external export bodies, raw Dataset cells, or
  transient staging/WAL/lock data;
- is not the same operation as exporting a Dataset or Artifact.

The exact file/container format, compression, encryption, remote destination,
and transport remain implementation decisions for AUD-A1/OPS. None may weaken
tenant isolation, boundedness, deterministic digesting, or redaction.

## Compatibility and downstream obligations

| Downstream node | Required AUD-C0 input |
| --- | --- |
| `AUD-A1` | Versioned envelope, actor/object/time/type filters, bounded pagination, lineage references, cursor binding, authorization, and audit export semantics |
| `OPS-O2` | Audit-specific retention/tombstone rules; no generalization into Snapshot/Artifact/Event/Run/Dataset GC |
| `AUT-C0` / `AUT-J1` / `AUT-A1` | Service-account actor identity, request/correlation links, capability checks, and redaction |
| `OPS-O4` | Local/server trust boundary and explicit system actor behavior |
| `H2` | Evidence of append-only integrity, recovery, tenant isolation, cursor isolation, and secret-free output |
| `H3` | Release evidence for version compatibility, migration, retention, export, and rollback behavior |

Downstream implementations may add compatible optional fields only under an
explicit versioned decision. They may not add a second ordering authority,
weaken workspace authorization, expose raw values, or silently merge audit and
control-plane event families.

## Non-goals and unresolved implementation decisions

This node does not implement an audit table or migration, writer, query API,
lineage materializer, retention scheduler, export file format, encryption,
identity provider, HTTP/IPC route, metrics/tracing backend, or UI. Storage
placement, index design, physical archive format, encryption/key management,
provider choice, and exact policy durations remain downstream decisions subject
to this contract and independent acceptance.
