# Issue #191 Implementation Contract: unified control plane (E5-C0)

> Status: Proposed; docs-only freeze for independent exact-head acceptance
> Revision: E5-C0-R1
> Risk: High
> Issue: #191
> Epic / current execution board: #81
> Entry base: `main@23ea7a2e2b3feacd4bdfbca06c81bc33c9c65bd9`
> Functional lineage anchor: `main@6dcec4fa35d3c46abe3c0c4abe8138263493d27c`
> Branch: `agent/issue-191-e5-c0-control-plane-contract`
> Worktree: `/home/owl/stillflow-e5-c0`
> Last updated: 2026-08-31

This document is the normative E5-C0 contract for the unified Runtime / Job /
API control plane. It freezes ownership, cardinality, references, lifecycle,
idempotency, events, bounded execution, artifacts, Preview provenance, and
Plan version concurrency before E5 implementation work begins.

It does not authorize Rust runtime code, HTTP handlers, database migrations,
dependency or lockfile changes, workflow changes, frontend changes, or any
follow-up E5 implementation task.

## 1. Authority, evidence, and scope

### 1.1 Authority order

For this contract the authority order is:

1. this document for E5-C0 control-plane semantics;
2. the stable logical, execution, storage, profiling, and export contracts
   linked in section 14 for their existing domains;
3. the current implementation facts recorded in section 1.2;
4. the historical discovery inventory, which is evidence only and is not a
   substitute for a frozen E5 decision.

An implementation may add a representation only when it preserves the rules
here. A handler, database schema, connector, or executor must not introduce a
second interpretation of a Job, Run, Event, Artifact, Preview, or PlanVersion.

### 1.2 Current implementation facts re-read at the entry base

The following facts were re-read from `main@23ea7a2e2b3feacd4bdfbca06c81bc33c9c65bd9`:

- `stillflow-core` owns the stable domain values currently present for
  `Session`, `SourceConnection`, `SourceAsset`, `Dataset`, `DatasetSnapshot`,
  `LogicalSchema`, `RequestContext`, and ingestion events.
- `stillflow-plan` owns the validated `LogicalPlan` DAG. Its `canonical_bytes()`
  are deterministic serialized plan bytes; its `PlanFingerprint` is the
  versioned non-security cache/index fingerprint
  (`stillflow-fnv1a64x4-v1`).
- `stillflow-engine` owns preflight, execution, cancellation/deadline
  propagation, Polars cleaning, profiling, quality, and the existing run gate.
  `MAX_ENGINE_CONCURRENT_RUNS` is 4; the default execution deadline is 15
  minutes and the maximum is 30 minutes.
- `stillflow-storage` owns SQLite control metadata, immutable Parquet snapshot
  partitions, Snapshot manifests, recovery, and atomic VerificationBundle
  publication. Existing report partitions are bounded to 1,024 rows and 2 MiB
  per report pack; the bundle reader is opened only through bundle membership.
- `stillflow-api` is currently only a crate boundary/smoke surface. It owns
  transport translation when implemented, not domain semantics.
- Existing connector and engine Preview surfaces return bounded Arrow envelopes.
  Engine Preview has 1,000 / 8 MiB defaults, 10,000 / 50 MiB maxima, a 30-second
  deadline, and bounded source scanning. Preview does not publish a Snapshot.
- Existing `Session`, `Dataset`, `IngestionEvent`, and `LogicalPlan::PlanNodeId`
  constructors still generate some IDs or timestamps internally. E5 freezes the
  required caller-injection behavior for future durable control-plane commands;
  this docs-only PR does not rewrite those constructors.

The older [E5 runtime domain inventory](e5-runtime-domain-inventory.md) was
useful for discovery but explicitly says that the E5 contract was not frozen.
Its old baseline must not be used as the current main base.

### 1.3 In scope

- Workspace, Session, Plan, PlanVersion, Job, Run, Event, and Artifact
  ownership and cardinality.
- References to SourceConnection, SourceAsset, Dataset, Snapshot, and
  VerificationBundle.
- State machines, terminal immutability, duplicate submission, caller-injected
  identities/clocks, event order/redaction/retention, queue/run bounds,
  cancellation, restart reconciliation, and bounded artifact reads.
- Preview as a provenance-only operation and the authoritative Plan digest and
  optimistic concurrency rules.

### 1.4 Explicit non-goals

- E5-S1 Runtime implementation, E5-J1 Job implementation, E5-A1 API
  implementation, E5-E1 event implementation, or E5-G1 end-to-end work.
- Axum routes, request/response structs, SQL migrations, queues, workers,
  schedulers, authorization, or generated clients.
- Changes to Issue #151, its temporal-upstream behavior, or its default feature
  status.
- Golden E2E, X-A1, E5 implementation code, new dependencies, or CI/workflow
  changes.

## 2. Planes and dependency direction

The control plane is a domain contract spanning the existing layers, not a new
crate dependency. The allowed dependency arrows remain:

```text
stillflow-api
      -> stillflow-engine
          -> stillflow-plan
          -> stillflow-connectors
          -> stillflow-storage
              -> stillflow-core
          -> stillflow-core
      -> stillflow-core
```

The following ownership rules are mandatory:

| Concern | Sole semantic owner | Allowed consumers |
| --- | --- | --- |
| Stable object identity, references, errors, schemas, event value types | `stillflow-core` | all higher layers |
| Logical operators, Plan validation, canonical plan bytes, Plan fingerprint | `stillflow-plan` | engine, control-plane services |
| Source capabilities and bounded connector streams | `stillflow-connectors` | engine |
| Preflight, execution, cancellation/deadline propagation, profiling, quality | `stillflow-engine` | control-plane service through typed calls |
| Durable Job/Run state, Event sequence, idempotency index, Artifact metadata | control-plane persistence service using `stillflow-storage` | API, engine orchestration |
| Snapshot and VerificationBundle physical publication/recovery | `stillflow-storage` | engine/control-plane service |
| HTTP status, JSON naming, headers, pagination transport | `stillflow-api` | external clients |

`stillflow-core` must remain free of Polars, DuckDB, SQLx, Axum, filesystem
paths, and transport-specific lifecycle behavior. `stillflow-api` may map a
domain result to HTTP, but may not invent a state transition, retry, digest,
or authorization decision that is absent from this contract.

## 3. Object model, ownership, and cardinality

IDs below are UUID-shaped opaque identifiers. A reference does not transfer
ownership. Every child is rejected if its parent reference is missing,
archived where creation is forbidden, or belongs to another Workspace.

| Object | Owner / parent | Cardinality and invariant |
| --- | --- | --- |
| Workspace | tenant/root boundary | One Workspace owns zero or more Sessions, SourceConnections, Plans, and their descendants. Workspace is the isolation key for idempotency and authorization. |
| Session | exactly one Workspace | A Session belongs to exactly one Workspace and may reference zero or more SourceConnections. It is the control context for Jobs and provenance, not an execution attempt. |
| SourceConnection | exactly one Workspace | A connection belongs to one Workspace and may have zero or more SourceAssets. Credential values are never part of the object; only `CredentialRef` is allowed. |
| SourceAsset | exactly one SourceConnection | An asset belongs to one connection and therefore one Workspace. Its connection identity cannot be changed in place; replacement is a new asset. |
| Dataset | exactly one Session and one source input identity | A Dataset is a logical registration in one Session. It may have zero or more immutable Snapshots. The source asset reference is immutable. |
| Snapshot | exactly one Dataset; produced by one Run | A Snapshot is immutable data plus manifest/provenance. It may be a member of at most one VerificationBundle. Snapshot visibility is separate from Job/Run state. |
| Plan | exactly one Workspace | A Plan is the stable authoring identity. It has one or more PlanVersions over time and no mutable executable body of its own. |
| PlanVersion | exactly one Plan | A PlanVersion is immutable after publication. A Plan has at most one current published version, while superseded versions remain addressable while retained. |
| Job | exactly one Session and one PlanVersion | A Job is one logical client submission. It has exactly one idempotency identity and zero or one Run: zero while it fails before execution starts, one once execution starts. E5-C0 does not authorize in-place retries. |
| Run | exactly one Job | A Run is one execution attempt and is created atomically when the Job starts. It references the exact PlanVersion and resolved input versions used by that attempt. |
| Event | exactly one Job or Run stream | An Event belongs to one stream, has one sequence within that stream, and may reference other objects without owning them. State-transition events are append-only. |
| Artifact | exactly one Run | An Artifact is an immutable, writer-digested output owned by one Run. It may be referenced by a VerificationBundle membership record; a reference does not create a second owner. |
| VerificationBundle | exactly one Run | A bundle is one atomic composite publication. It has one bundle-level provenance identity and references exactly one Run, one accepted Snapshot, one validation report, one deduplication report, and optionally one rejected-rows artifact. |
| ArtifactReadHandle | no durable owner; scoped to one caller and one Artifact | A handle is an opaque, bounded cursor over one committed Artifact. It cannot be reused for another Artifact, bypass membership, or read staging/tombstoned content. |
| PreviewProvenance | exactly one Preview operation and one Session | Provenance records input/plan/bounds/result identity only. Preview payload is not an Artifact, Snapshot, Job result, or Event payload. |

The control-plane graph is therefore:

```text
Workspace
├── Session ──┬── Job ─── PlanVersion ─── Plan
│             │    └── Run ─── Event stream
│             │         ├── Artifact(s)
│             │         └── VerificationBundle ─── Snapshot ─── Dataset
│             └── PreviewProvenance
└── SourceConnection ─── SourceAsset ─── Dataset input reference
```

The diagram shows references, not an instruction to store all objects in one
table. Physical representation is an implementation choice subject to these
cardinality and visibility invariants.

## 4. References and lineage

### 4.1 Typed input references

Every execution input is one of:

```text
AssetInput   = { source_asset_id, version_digest }
SnapshotInput = { snapshot_id, version_digest }
```

`version_digest` is SHA-256 over the versioned logical input descriptor,
including the input identity and authorized logical schema, never raw rows.
An implementation may include a source-specific version token inside the
descriptor only when it is stable, non-secret, and documented by that source.

### 4.2 Required lineage on a Run

A Run records, at minimum:

- `workspace_id`, `session_id`, and `job_id`;
- `plan_id`, `plan_version_id`, `canonical_plan_digest`, and the non-security
  `plan_fingerprint` if used as an index;
- the ordered set of resolved `AssetInput` / `SnapshotInput` values;
- the engine contract version and build identity used;
- produced Snapshot, Artifact, and VerificationBundle references, if any.

The ordered input set and all digests are immutable once the Run starts. A
different input version, PlanVersion, or canonical digest is a different Run
request and cannot be silently substituted by a worker.

### 4.3 Existing E4/E2 lineage compatibility

The existing `ArtifactProvenanceInput`, `LogicalInputRef`, `SourceRowRef`,
`RuleRef`, `DatasetSnapshot`, and `VerificationBundleMembership` remain valid
and are subordinate to this graph:

- E4 artifact provenance `run_id` is the E5 Run identity.
- E4 `session_id` is the E5 Session identity.
- E4 `canonical_plan_digest` is the authoritative PlanVersion content digest.
- E4 accepted Snapshot remains a Snapshot child of the Dataset and a produced
  output of the Run, not a second control-plane owner.
- E4 bundle membership remains atomic; no child is visible through an
  uncommitted or failed bundle.

## 5. Identity, clocks, and idempotency

### 5.1 Caller-injected durable identity and time

The command boundary must supply or inject through one deterministic identity
provider all durable IDs: `workspace_id`, `session_id`, `plan_id`,
`plan_version_id`, `job_id`, `run_id`, `event_id`, `artifact_id`, `bundle_id`,
and `snapshot_id`. Domain constructors and persistence code must not silently
replace them with random IDs.

The same command boundary supplies UTC timestamps for `created_at`, `queued_at`,
`started_at`, `finished_at`, `committed_at`, `tombstoned_at`, and event
`occurred_at`. A timestamp provider may be server-side, but it is passed into
the domain command explicitly. Timestamp order is checked, not repaired.

Required ordering for a started Run is:

```text
object created_at <= job queued_at <= run started_at <=
run finished_at <= artifact/bundle committed_at
```

Fields that do not apply are absent, not set to a guessed zero time. A failed
or cancelled Run may have no committed output and therefore has no
`committed_at`.

### 5.2 Idempotency key

Job submission carries:

```text
idempotency_scope = (workspace_id, operation = "job.submit", key)
request_digest    = SHA-256(canonical logical submission descriptor)
```

The key is non-empty, UTF-8, at most 128 bytes, and is compared byte-for-byte
after normalization is rejected; case folding and whitespace trimming are not
applied. The canonical descriptor includes the Session, PlanVersion, ordered
input references, execution policy, and requested output policy. It excludes
transport headers and server-generated timestamps.

The durable idempotency record stores the key, request digest, original Job ID,
and the original submission result. Its retention cannot be shorter than the
Job record. The outcomes are exhaustive:

| Submission | Result | Mutation |
| --- | --- | --- |
| Key absent | Create one queued Job and one submission event | Exactly one new Job |
| Same scope/key and same request digest | Replay the original Job ID/state/result | No new Job, Run, or state event |
| Same scope/key and different request digest | `IdempotencyConflict` with the original Job ID if disclosure is authorized | No mutation |
| Queue is full before create | `QueueFull` | No Job or idempotency record |

Duplicate replay is not a retry. A caller that wants a new attempt must submit a
new Job with a new key. This makes duplicate, restart, and failure outcomes
deterministic in E5-C0.

### 5.3 Terminal immutability

`Succeeded`, `Failed`, and `Cancelled` are terminal for Job and Run. Their
state, input references, PlanVersion, error class, result references, and
terminal timestamps cannot be edited by normal commands. A duplicate request
returns the original terminal record. Maintenance may transition retained
Artifacts or Snapshots to a tombstoned storage state, but may not rewrite their
digest, provenance, ownership, or historical Job/Run outcome.

The first compare-and-set terminal transition committed for a Job/Run wins.
Concurrent cancellation, completion, failure, and duplicate requests never
overwrite that winner.

## 6. Lifecycle states and complete transitions

State names are lower-case wire values. An omitted arrow is forbidden. Every
transition emits the specified stream event in the same durable transaction as
the state mutation.

### 6.1 Workspace and resource states

| Object | States | Allowed transitions | Terminal/creation rule |
| --- | --- | --- | --- |
| Workspace | `active`, `archived` | `active -> archived` | Archived Workspaces accept no new child or Job; retained reads remain possible subject to authorization/retention. |
| Session | `open`, `closing`, `closed` | `open -> closing`, `closing -> closed`, `open -> closed` for an empty session | A closed Session cannot accept a new Job. Closing is rejected while a non-terminal Job exists unless the caller first cancels it. |
| SourceConnection | `active`, `disabled`, `retired` | `active -> disabled`, `disabled -> active`, `disabled -> retired` | Retired identity is never reused; assets remain historical references. |
| SourceAsset | `active`, `retired` | `active -> retired` | Retired assets may be read for retained lineage but cannot start a new Job. |
| Dataset | `active`, `archived` | `active -> archived` | Archived Dataset accepts no new Snapshot; existing Snapshot lineage is retained. |
| Plan | `active`, `archived` | `active -> archived` | Archived Plan accepts no new PlanVersion or Job. Existing versions remain immutable while retained. |
| PlanVersion | `draft`, `published`, `superseded`, `archived` | `draft -> published`, `published -> superseded`, `superseded -> archived` | Published, superseded, and archived versions are immutable. A draft is not executable. |

### 6.2 Job state machine

```text
queued ───────────────> running ───────────────> succeeded
   │                       │  \                    terminal
   │                       │   └───────────────> failed
   │                       │
   ├───────────────────> cancelling ───────────> cancelled
   │                       │  \                 terminal
   └───────────────────> failed └──────────────> failed
```

Allowed Job transitions are exactly:

| From | To | Meaning |
| --- | --- | --- |
| `queued` | `running` | A worker atomically claimed the Job and created its Run. |
| `queued` | `cancelling` | Cancellation won before worker start. |
| `queued` | `failed` | Durable admission/preflight failed before a Run was created. |
| `running` | `cancelling` | Cancellation was accepted while the Run was active. |
| `running` | `succeeded` | The Run succeeded and all required outputs are committed. |
| `running` | `failed` | The Run produced a terminal failure. |
| `cancelling` | `cancelled` | Cleanup and cancellation are confirmed. |
| `cancelling` | `failed` | Cleanup cannot be confirmed or restart reconciliation detects worker loss. |

`cancelling -> succeeded` is forbidden. If completion commits first, the Job
is `succeeded` and a later cancel is an already-terminal response. If cancel
commits first, completion must not publish a successful Job result.

### 6.3 Run state machine

A Run is created only with its Job's `queued -> running` transition. Allowed
transitions are:

```text
running ───────> succeeded
   │  \              terminal
   │   └──────────> failed
   └─────────────> cancelling ─────> cancelled
```

Allowed transitions are `running -> succeeded`, `running -> failed`,
`running -> cancelling`, and `cancelling -> cancelled` or
`cancelling -> failed`. A Run cannot return to `queued`, be reused for a
retry, or be marked successful after cancellation has won.

### 6.4 Artifact and bundle states

Artifacts and VerificationBundles use:

```text
staged -> committed -> tombstoned
staged -> failed
```

Staged and failed records are not readable. `committed` is the sole readable
state. Tombstoning is a retention operation and is idempotent; it removes
readability without changing the original provenance/digest record retained
for audit. Failed publication removes uncommitted files and visibility rows.

### 6.5 Terminal result rules

- Job `succeeded` requires Run `succeeded` and all required output publication
  transactions committed.
- Job `failed` carries exactly one stable failure category and a sanitized
  summary; partial outputs are not returned as the Job result.
- Job `cancelled` requires Run `cancelled` or a queued Job cancelled before a
  Run existed. It must not expose a staged or partially published output.
- A terminal Run has exactly one terminal event and one terminal timestamp.
- State reads are snapshots. They never infer success from files, infer failure
  from a missing file, or revive a state after restart.

## 7. Events: order, payload, redaction, and retention

### 7.1 Event stream and record

There are two lifecycle stream kinds: `job` and `run`. Each Event has:

```text
event_id, workspace_id, session_id,
stream_kind, stream_id, sequence,
event_type, event_version, occurred_at,
job_id, optional run_id,
request_id, correlation_id, actor_ref,
sanitized_payload
```

`sequence` is assigned by the persistence transaction, starts at 1, is
strictly increasing and gap-free within a stream, and is unique on
`(stream_kind, stream_id, sequence)`. A failed transaction consumes no
sequence. Event IDs and timestamps are caller-injected as in section 5, but a
caller cannot choose or skip a stream sequence.

Each allowed lifecycle transition emits one event on the affected stream. A
Job start creates the Run and writes the Job `running` event and Run `running`
event in one transaction. A duplicate replay emits no Job/Run state event. A
restart reconciliation emits exactly one recovery event per state that is
changed.

### 7.2 Event types

The initial stable event type set is:

```text
job.queued
job.running
job.cancelling
job.succeeded
job.failed
job.cancelled
run.running
run.cancelling
run.succeeded
run.failed
run.cancelled
run.reconciled
artifact.committed
artifact.tombstoned
```

Event types are versioned values, not Rust enum names exposed by HTTP. New
types require a contract revision. A state transition must not be represented
by an untyped log message.

### 7.3 Ordering and resume

Consumers resume with an opaque cursor containing stream identity and the last
observed sequence. The service returns at most 1,000 events per page. A cursor
from another Workspace or stream is rejected. Events are ordered by sequence,
not by wall-clock timestamp; timestamps are for audit only and may tie.

### 7.4 Redaction and bounded payload

The sanitized payload is metadata only and is capped at 64 KiB encoded size.
It may contain IDs, state, counts, stable error categories, retryability,
schema fingerprints, digests, and bounded user labels. It must never contain:

- passwords, access keys, tokens, raw connection strings, or secret-bearing
  configuration;
- raw dataset rows, Preview batches, full validation/rejected payloads, or
  arbitrary exception backtraces;
- unbounded nested JSON, executable code, SQL, or credentials hidden under a
  user-defined key.

Existing `ensure_safe_event_metadata` and sanitized error behavior remain the
minimum security boundary. Redaction is fail-closed: a payload that cannot be
proven safe is rejected and no event is written.

### 7.5 Retention

Events are retained at least as long as their Job/Run and any referenced
Artifact are retained. A stream cannot be physically removed while its
terminal Job, Run, Artifact, Snapshot, or legal/audit hold remains addressable.
Retention cleanup is maintenance, not a lifecycle rewrite. It must preserve a
stable tombstone/absence result and must never reuse a stream sequence.

## 8. Queue, execution, cancellation, and restart

### 8.1 Frozen bounds

E5-C0 freezes these v1 control-plane bounds:

| Resource | Bound | Result at the bound |
| --- | --- | --- |
| Accepted queued Jobs per Workspace | 256 non-terminal queued Jobs | New submission returns `QueueFull` with no mutation. |
| Active engine Runs per process | 4, matching `MAX_ENGINE_CONCURRENT_RUNS` | Start returns `Busy`; the Job remains `queued` unless cancellation or a defined admission failure wins. |
| Execution deadline | 15-minute default, 30-minute maximum | A longer request is rejected before Run start; expiration is `failed` with category `timeout`. |
| Preview deadline | 30 seconds | Expiration is `preview.failed` with category `timeout`; no Job/Run/Artifact is created. |
| Event payload | 64 KiB encoded | Oversize or unsafe event is rejected; no partial event. |
| Event page | 1,000 records | The caller must resume with the returned cursor. |
| Artifact read page | 1,024 rows and 2 MiB encoded payload | The reader returns a bounded page and cursor; no whole-artifact response. |
| Report pack | 1,024 rows and 2 MiB, as existing storage contract | Storage rejects a violating partition before publication. |

Existing lower-level limits remain in force: Arrow batches are bounded by the
core envelope limits, Snapshot storage retains its 16,384 partition / 1e9 row
/ 1 TiB ceilings, and Engine Preview retains its 10,000-row / 50 MiB output
and 100,000-row / 64 MiB source scan ceilings. E5 cannot widen a lower-level
bound by accepting a larger API request.

### 8.2 Admission and worker claim

Submission performs validation, idempotency lookup, queue-cap check, and Job
creation atomically. A worker claim performs a compare-and-set
`queued -> running`, creates exactly one Run, records its resolved input and
execution identity, and appends the start events atomically. A second worker
gets `AlreadyClaimed`/`Busy` and cannot create another Run.

The queue is FIFO by `(queued_at, job_id)` within a Workspace after durable
ordering. A future scheduler may implement fairness, but it may not reorder
already committed sequence facts or create a second queue semantic.

### 8.3 Cancellation

Cancellation is a command with its own request ID and idempotency of the
command result. Its behavior is:

- `queued`: transition to `cancelling`, then `cancelled` in the same transaction
  when no worker claim can race; no Run is created.
- `running`: transition to `cancelling`, cancel the existing
  `RequestContext`, and wait for the worker to confirm cleanup.
- `cancelling`: replay the current cancellation result; do not create another
  event or Run.
- `succeeded`, `failed`, or `cancelled`: return the terminal record unchanged.

Cancellation is cooperative. Connector reads and engine operations receive the
existing `RequestContext`; a worker must check it at input, bounded batch, and
publication boundaries. A cancellation request is not proof that data stopped;
the terminal `cancelled` state is proof only after cleanup has completed.

### 8.4 Failure and retry

Every failed submission or execution has exactly one stable category, sanitized
message, retryability bit, and failure timestamp. E5-C0 forbids automatic or
in-place retries. A user-visible retry is a new Job with a new Job ID and
idempotency key; it may reference the same retained PlanVersion and input
versions.

### 8.5 Restart reconciliation

Recovery runs before accepting new worker claims and uses the persistence
maintenance gate. It applies this exhaustive rule:

| Durable state at process restart | Reconciliation |
| --- | --- |
| queued Job | Remains queued with the same Job ID, queue timestamp, and event sequence. |
| running Job/Run | Run becomes `failed` with category `worker_lost`; Job becomes `failed`; one `run.reconciled` and one `run.failed`/`job.failed` transition record are committed as appropriate. |
| cancelling Job/Run | Run becomes `failed` with category `worker_lost`, not `cancelled`, unless cancellation had already committed its terminal transition. This avoids claiming cleanup that was not observed. |
| succeeded/failed/cancelled Job or Run | Remains unchanged. |
| staged Snapshot/Artifact/Bundle without committed visibility | Storage recovery removes the unpublished staging/journal residue; no Job or Run becomes successful from files alone. |
| committed Snapshot/Artifact/Bundle with terminal references | Remains readable and is reconciled only as an already committed result. |

Recovery is idempotent. Re-running it cannot append a second terminal event,
create a second Run, or delete a committed output. An operator may submit a new
Job after `worker_lost`; the old Job/Run remains the authoritative failed
attempt.

## 9. Artifact ownership, publication, retention, and reads

### 9.1 Artifact identity and provenance

Every Artifact has one immutable identity, kind, owner Run, content digest,
schema/schema fingerprint where applicable, summary, created/committed times,
and provenance. The writer, not the caller, computes content digests and
derived summaries. Caller-supplied digest or summary values are draft inputs
only and must not override writer-computed values.

The existing Artifact kinds remain the vocabulary for verification outputs:
`VerificationBundle`, `AcceptedSnapshot`, `ValidationReport`, `RejectedRows`,
and `DeduplicationReport`. Accepted Snapshot is physically a Snapshot and
logically a bundle child; it is not a second Artifact owner.

### 9.2 Atomic publication

Visibility is all-or-nothing at the bundle boundary:

1. validate the full draft and identity distinctness;
2. create journal/staging state;
3. write bounded partitions and manifests;
4. verify digests, lineage, schema, row/byte/partition bounds;
5. install final files before control-plane visibility;
6. commit membership, provenance, and visibility in one transaction;
7. remove staging only after commit.

Readers see the complete committed bundle or none of it. A failed or cancelled
Run cannot expose a partial Snapshot, report, rejected-row payload, or
VerificationBundle. Existing `SnapshotStore` and VerificationBundle recovery
rules are the physical implementation reference and remain authoritative for
their existing storage surfaces.

### 9.3 Retention and tombstones

Retention is applied only to committed outputs. A committed Artifact or
Snapshot may become tombstoned only when its retention deadline has elapsed,
no active Job/Run or bundle reference requires it, and no hold blocks removal.
Tombstoning is idempotent and recorded as maintenance evidence. Tombstoned
content is not readable; the retained metadata must not claim it is committed
and readable.

No cleanup operation may unlink a file belonging to an active publication. The
existing storage root lock, activity gate, journal, and recovery protocol are
the minimum safety mechanism.

### 9.4 Bounded read handles

An Artifact read request must name one committed Artifact and receive an opaque
handle bound to `(workspace_id, artifact_id, content_digest, authorization
context)`. Each page request has at most 1,024 rows and 2 MiB encoded payload.
The handle returns an opaque continuation cursor and a `has_more` flag.

The reader must:

- verify the Artifact is still committed and the digest/generation matches;
- read through bundle membership for verification children;
- never expose staging, tombstoned, or unrelated artifact data;
- never materialize the whole Artifact in API memory;
- fail closed on corrupt manifest, digest mismatch, schema mismatch, or bound
  exhaustion.

The handle itself is not durable business state and expires according to the
transport session/retention policy. Reopening an Artifact creates a new handle
at the beginning; a cursor cannot be used to skip authorization or retention.

## 10. Preview is provenance-only

Preview remains a bounded diagnostic operation and is not a disguised import.
There are two existing surfaces: connector asset Preview and engine node-level
Preview. They retain their existing typed request/result boundaries and share
the existing cancellation/deadline/run-gate semantics.

A Preview may record one `PreviewProvenance` entry containing:

- Preview operation ID, Workspace/Session, request ID, and completion/failure;
- input Asset/Snapshot reference and input version digest;
- Plan ID/PlanVersion ID, canonical plan digest, non-security fingerprint, and
  selected target node;
- schema fingerprint, row/byte/source-scan bounds, observed counts, truncation
  flags, and warning/error categories;
- created/started/finished timestamps and engine contract/build identity.

The following are forbidden in Preview provenance, Events, or ordinary control
metadata: Arrow batches, raw rows, rejected payloads, full connection strings,
or any secret. Preview payload is returned only in the bounded request response;
it is not persisted as a Snapshot, Artifact, Job, Run result, or replayable
event payload. A caller that wants durable output must submit a Job.

Preview success or failure does not advance Dataset/Snapshot state. A Preview
with a duplicate request ID may replay its bounded provenance/result only while
the response/provenance retention policy permits; it must not create a Job or a
Job/Run Event. If audit emission is required later, it must use a separately
specified Session audit stream rather than silently widening the Job/Run event
contract.

## 11. Plan, PlanVersion, digest, and optimistic concurrency

### 11.1 PlanVersion contents

A PlanVersion contains:

```text
plan_id, plan_version_id, workspace_id,
version_number, parent_version_id,
logical_plan_version,
canonical_plan_bytes,
canonical_plan_digest,
plan_fingerprint,
state, created_at, published_at, archived_at
```

`canonical_plan_bytes` are the validated bytes returned by the existing
`LogicalPlan::canonical_bytes()`. `canonical_plan_digest` is the sole integrity
identity:

```text
canonical_plan_digest = SHA-256(canonical_plan_bytes)
```

The digest is lowercase hexadecimal when serialized. No timestamp, Job ID,
Workspace ID, transport representation, or engine build is included in the
PlanVersion content digest. `plan_fingerprint` may accelerate lookup but is
not cryptographic, not authorization evidence, and not sufficient to prove
semantic equality; a fingerprint hit must compare canonical bytes/digests.

### 11.2 Publication and immutability

Creating a draft validates the full LogicalPlan DAG, node references, schema
references, expression shape, and supported contract version. Publishing
requires a non-empty canonical byte sequence and its matching SHA-256 digest.
Publishing a PlanVersion does not mutate its bytes. Superseding changes only
the Plan's current-version pointer and the old version's lifecycle state; it
does not edit the old version.

### 11.3 Optimistic concurrency

Plan writes carry `expected_current_version_id` (or the equivalent exact
version token). The write succeeds only when the stored current version still
equals that token. A mismatch returns `VersionConflict` with the current
version identity and does not create a partially published version.

The API may expose this as an HTTP `If-Match`/ETag mechanism, but HTTP headers
are a transport mapping, not the concurrency semantic. The ETag must bind the
exact PlanVersion ID and canonical digest, never only the non-security
fingerprint. A concurrent publish is therefore one successful append and one
deterministic conflict, not last-writer-wins.

### 11.4 Job binding

A Job binds to one exact published PlanVersion and canonical digest at
submission. It must fail closed if the version is missing, archived, or its
stored bytes no longer hash to the recorded digest. Publishing a newer version
does not alter a queued Job's binding.

## 12. Domain/API boundary and error mapping

Domain commands and typed results are the source of semantics. The eventual API
may use mappings such as:

| Domain result | Transport mapping guidance |
| --- | --- |
| queued/running/cancelling | `202` for submit/accepted command; status reads return the state |
| succeeded/failed/cancelled | `200` status/result with immutable terminal record |
| IdempotencyConflict or VersionConflict | `409` |
| QueueFull or Busy | `429` or a documented busy response |
| not found / unauthorized | the repository's authorization policy decides disclosure; handlers do not infer object ownership |
| invalid plan/input/deadline | `400`/`422` according to the API contract |
| timeout/cancelled/worker_lost | typed terminal Job/Run result, not a transport-only error |

The exact HTTP status table may be refined by E5-A1 without changing the domain
state machine. No handler may directly set a database state to make a response
look successful, retry a failed command without an idempotency decision, or
serialize raw Arrow data into an ordinary JSON control response.

## 13. Acceptance matrix and non-implementation gate

Independent acceptance must verify each key against this document and the
exact PR head:

| Registry key | Objective evidence |
| --- | --- |
| `e5-c0:ownership-cardinality` | Sections 2 and 3 contain one owner and explicit cardinality for every named object. |
| `e5-c0:refs-lineage` | Sections 4 and 9.1 bind Asset/Snapshot inputs, PlanVersion, Run, Snapshot, Artifact, and VerificationBundle lineage. |
| `e5-c0:lifecycle-transitions` | Section 6 lists every state and every allowed transition for Workspace/resources, Job, Run, and publication objects. |
| `e5-c0:terminal-idempotency` | Sections 5.2, 5.3, 6.5, and 8.4 define same-key replay, conflict, terminal winner, failure, and retry outcomes. |
| `e5-c0:event-order-redaction` | Section 7 defines event fields, stream sequence assignment, event types, cursor ordering, 64 KiB redaction, and retention. |
| `e5-c0:queue-run-restart` | Section 8 defines queue/active/deadline/Preview bounds, claim, cancellation, failure, and exhaustive restart reconciliation. |
| `e5-c0:artifact-read` | Section 9 defines owner, writer-computed digest, atomic visibility, tombstones, and 1,024-row/2 MiB read pages. |
| `e5-c0:preview-provenance` | Section 10 states the provenance fields and explicitly forbids persisted Preview payload. |
| `e5-c0:plan-digest-concurrency` | Section 11 defines canonical bytes, SHA-256 authority, fingerprint non-authority, immutable versions, and compare-and-set publication. |
| `e5-c0:no-runtime` | The PR diff contains only this Markdown contract; no Rust, Cargo, frontend, workflow, generated, or unrelated roadmap file changes. |

The following checks are required for this docs-only PR:

- `git diff --check` passes.
- Every repository-relative Markdown link in this document resolves at the
  exact base, including the inventory and existing contracts.
- All issue/PR references are intentional and point to #191, #81, or the
  linked historical/current contracts; no invented issue number is used.
- The dependency arrows in section 2 do not reverse the repository's existing
  layer direction.
- The exact diff is one new Markdown contract file under `docs/issues/`; no
  runtime implementation, dependency, workflow, frontend, or checklist
  refresh is included in E5-C0.
- The implementation PR remains Draft until an independent reviewer publishes
  an exact-head acceptance. This task does not mark Ready, merge, or close #191.

## 14. References and consequences

### 14.1 Repository references

- [ADR-001: logical, physical, and storage boundaries](../architecture/adr-001-logical-physical-and-storage-boundaries.md)
- [ADR-002: deterministic runtime and physical executors](../architecture/adr-002-deterministic-runtime-and-physical-executors.md)
- [ADR-003: profiling, quality, and findings](../architecture/adr-003-profiling-quality-and-findings.md)
- [ADR-004: export and output artifacts](../architecture/adr-004-export-and-output-artifacts.md)
- [E5 runtime domain inventory](e5-runtime-domain-inventory.md)
- [Storage publication and recovery inventory](storage-publication-recovery-inventory.md)
- [Backend completion execution checklist](../development/backend-completion-execution-checklist.md)

### 14.2 Explicit consequences

- The first E5 runtime must add a durable control-plane service that reuses
  existing storage publication/recovery and engine RequestContext/run-gate
  behavior; it must not create a second queue, event, or retry system.
- Current generated IDs/timestamps in older core constructors are known
  transitional facts. E5 implementation must introduce explicit command-level
  injection and reconcile those constructors before claiming the corresponding
  acceptance key.
- Automatic retry, in-place Run reuse, persisted Preview payloads, mutable
  PlanVersion bodies, fingerprint-only integrity, whole-artifact API reads, and
  handler-defined lifecycle semantics are deliberately rejected.
- Workspace authorization and credential-provider lifecycle remain separate
  security work. This document supplies Workspace as the tenant/reference
  boundary without implementing RBAC or changing Issue #151.

This contract is complete for E5-C0 when the independent exact-head reviewer
can reproduce the matrix in section 13 and confirms that no implementation
surface was changed.
