# E5-J2-C0: Typed JobOperation v1 contract

> Status: Proposed; docs-only contract freeze for independent exact-head review
> Issue: [#233](https://github.com/X44421/stillflow/issues/233)
> Roadmap authority: [#81](https://github.com/X44421/stillflow/issues/81)
> Entry base: `main@babf89e294aaab7a8a84c432b5b52fe382b84b8d`
> Target branch: `agent/issue-233-e5-j2-c0-job-operation-contract`
> Contract revision: `E5-J2-C0-R1`
> Last updated: 2026-09-02

This document freezes one typed, durable, versioned `JobOperation v1` for the
existing E5 Job/Run authority. It defines the smallest semantic bridge between
the existing Materialize, Verification, Profile, and Export runtimes and one
durable E5 Job/Run lifecycle.

This is a contract only. It does not implement a bridge, add a storage
migration, change the API or route/schema manifest, add an Event type, or
promise that the current `main` can submit all four operations. A later
implementation task must satisfy this contract without creating a second Job,
Run, Event, queue, retry, digest, or publication authority.

## 1. Authority, evidence, and scope

### 1.1 Authority order

The authority order for this contract is:

1. this document for `JobOperation` kind/version, operation-specific input and
   output semantics, and the operation-to-lifecycle binding;
2. the unified control-plane contract in
   [`issue-191-unified-control-plane-contract.md`](issue-191-unified-control-plane-contract.md)
   for Workspace, Session, PlanVersion, Job, Run, Event, generic idempotency,
   lifecycle transitions, restart reconciliation, and bounded reads;
3. the existing domain contracts for the operation payloads:
   [`issue-054-validation-rejected-rows-contract.md`](issue-054-validation-rejected-rows-contract.md),
   [ADR-003](../architecture/adr-003-profiling-quality-and-findings.md), and
   [ADR-004](../architecture/adr-004-export-and-output-artifacts.md);
4. the transport-neutral E5-A1 API boundary and the E5-E1 Event Stream as
   projections of these domain rules, not alternative semantic authorities;
5. implementation facts re-read from the exact entry base.

An implementation may choose a physical representation only when it preserves
the typed fields, version checks, identity, cardinality, provenance, and
visibility rules here. An HTTP handler, JSON policy field, worker, or storage
reader must not reinterpret an operation.

### 1.2 Re-read implementation facts

The exact entry base already contains the E5-J1 durable Job/Run machinery and
typed storage records, including:

- `stillflow-storage::JobSubmission`, `JobRecord`, and `RunRecord`;
- `stillflow-engine::JobRuntime` with durable claim, cancellation, deadline,
  restart reconciliation, and the existing engine run gate;
- `stillflow-storage::ArtifactRefRecord` with staged, committed, tombstoned,
  and failed publication states;
- `stillflow-api`'s transport-neutral job, run, event, and artifact boundary;
- the durable E5-E1 event stream projection.

At this base, `JobSubmission`/`JobRecord` carry plan, input, execution-policy,
and output-policy fields but no typed operation field, and
`JobRuntime::execute_claimed` still resolves and invokes the Materialize path
(`backend/crates/stillflow-storage/src/control_plane.rs:JobSubmission`,
`backend/crates/stillflow-engine/src/job_runtime.rs:JobExecutionSpec`,
`backend/crates/stillflow-engine/src/job_runtime.rs:execute_claimed`). Those
facts describe the implementation starting point; they are not permission to
extend this PR into a bridge or control-plane migration.

### 1.3 Authorized scope of this freeze

The authorized change is exactly one new Markdown contract under
`docs/issues/`. This document may define typed shapes, state/output tables,
canonical identity inputs, provenance, bounds, compatibility rules, and later
implementation acceptance evidence.

The following are explicitly outside this PR:

- Rust, TypeScript, frontend, generated contract, API, route, or manifest code;
- SQLite/schema migrations, Cargo manifests or lockfiles, dependencies,
  workflows, or service/listener packaging;
- a JobOperation bridge, dispatch logic, resolver changes, or runtime
  orchestration;
- changes to the existing E5-C0, E4, Q, X, E5-A1, or E5-E1 documents;
- SQL connector #9, native DuckDB #10, SEC, AUD, AUT, OPS, or frontend work;
- Q-A1 full Profile/Quality/Drift API, X-A1 retention/delete API, or a
  reopening of [#232](https://github.com/X44421/stillflow/issues/232).

## 2. JobOperation v1 model

### 2.1 Closed typed union

`JobOperation` is a closed, typed union. The durable Job contains the
operation kind and version as first-class fields and a validated typed
descriptor. The operation is never represented only by a member of
`execution_policy`, `output_policy`, or an untyped JSON blob.

The v1 union is:

```text
JobOperation {
    operation_kind: OperationKind,
    operation_version: u16,
    descriptor: OperationDescriptorV1,
}

OperationKind = Materialize | Verification | Profile | Export

OperationDescriptorV1 =
    MaterializeV1 {
        source_asset: SourceAssetRef,
        materialize_policy: MaterializePolicyV1,
    }
  | VerificationV1 {
        snapshot: SnapshotRef,
        verification_policy: VerificationPolicyV1,
    }
  | ProfileV1 {
        snapshot: SnapshotRef,
        profile_request: ProfileRequestV1,
    }
  | ExportV1 {
        snapshot: SnapshotRef,
        export_request: ExportRequestV1,
    }
```

The common durable Job envelope continues to carry exactly one Workspace,
Session, published PlanVersion and canonical Plan digest as required by E5-C0.
The operation descriptor is additional typed domain data; it does not replace
the common PlanVersion binding, input lineage, execution policy, or output
policy.

For a post-materialization operation, the PlanVersion is the exact published
version that produced or authorizes the committed Snapshot. It is retained in
the Job/Run lineage and checked against the Snapshot provenance; it does not
cause the plan to execute a second time for Verification, Profile, or Export.

### 2.2 Durable representation and identity

The durable Job and Run must retain, or losslessly address through typed
columns, all of the following:

| Field | Rule |
| --- | --- |
| `operation_kind` | One of the four closed v1 kinds; stored separately from policy fields. |
| `operation_version` | `1` for this contract; stored separately and checked on every read/claim. |
| `operation_descriptor` | The validated variant-specific typed value; a serialized form must identify its type and version. |
| `operation_descriptor_digest` | SHA-256 of the canonical typed descriptor bytes; the writer recomputes it. |
| `request_digest` | E5-C0 submission digest extended with the operation kind, version, descriptor, and all common identity inputs. |
| Run copy | The Run records the same kind, version, descriptor digest, and resolved input identities; mismatch is a durable failure, never a worker choice. |

An implementation may normalize descriptor members into columns or store a
versioned canonical payload, but it must not make a policy JSON field the sole
source of operation meaning. A decoder that cannot validate the typed
descriptor must not claim or execute the Job.

### 2.3 Canonical operation and submission descriptors

The operation descriptor and the E5-C0 logical submission descriptor use one
deterministic canonical form:

- UTF-8, no insignificant whitespace, and lexicographically ordered object
  member names;
- arrays retain the contract-declared order; input and output reference order
  is semantic and is never sorted by a worker;
- enum values use their exact lower-camel wire names;
- UUIDs are canonical lowercase hyphenated strings; SHA-256 values are
  lowercase hexadecimal;
- integers are plain decimal integers; floating-point values are forbidden in
  JobOperation control descriptors unless a subordinate operation contract
  defines a bit-exact representation;
- only fields declared by the active version are accepted; unknown fields,
  duplicate fields, missing required fields, and ambiguous nulls fail closed.

`operation_descriptor_digest` is the SHA-256 digest of the canonical typed
descriptor, with the domain prefix `stillflow.job-operation.descriptor.v1`.
`request_digest` is the SHA-256 digest of the complete canonical logical Job
submission descriptor. It includes the fields in §3.2 and excludes the
idempotency key, transport metadata, caller display text, server-generated
timestamps, and event correlation values.

The operation kind and version therefore participate in idempotency identity
both directly and through the operation descriptor digest. A Materialize and
a Verification request can never collide as the same logical request merely
because they reuse a key or input identifier.

## 3. Identity, idempotency, and reference vocabulary

### 3.1 Common identity boundary

The existing E5-C0 idempotency scope remains:

```text
idempotency_scope = (workspace_id, "job.submit", idempotency_key)
```

The key is non-empty UTF-8 of at most 128 bytes and is compared byte-for-byte;
normalization, case folding, and whitespace trimming are not applied. The
operation kind is not a new idempotency scope and does not create a second
submission system. It is part of the request identity inside the existing
scope.

The complete `request_digest` inputs are:

1. `workspace_id`;
2. `session_id`;
3. the exact published `plan_id`, `plan_version_id`, and canonical Plan digest;
4. `operation_kind` and `operation_version`;
5. the canonical typed operation descriptor, including its ordered input
   references and operation-specific options;
6. the normalized, typed execution policy, including effective deadline and
   every bound that can affect admission or output;
7. the normalized, typed output policy, including format, destination policy,
   or publication options where the operation supports them.

The caller-injected `job_id`, `event_id`, `run_id`, output IDs, request ID,
correlation ID, actor reference, timestamps, and process identity are not
semantic request inputs. The first accepted submission owns the durable Job ID;
same-key/same-digest replay returns that original identity and result without
creating a new Job or Run.

The exhaustive outcomes are inherited from E5-C0:

| Submission | Result | Mutation |
| --- | --- | --- |
| New scope/key and valid v1 descriptor | One queued Job | One Job and one submission event |
| Same scope/key and same complete request digest | Replay original Job/result | No new Job, Run, output, or state event |
| Same scope/key and different digest, including a different operation kind/version | `IdempotencyConflict` | No mutation |
| Unknown operation/version or invalid descriptor | Typed validation failure | No Job, Run, ref, or event |
| Queue/bound admission failure | Typed bound failure | No Job or idempotency record |

Duplicate replay is not retry. A new attempt requires a new idempotency key
and produces a new Job identity under the same operation rules.

### 3.2 Typed reference forms

References are opaque typed identities, not ownership transfers. Every
reference is workspace-bound and carries the content/version digest needed to
revalidate the object at execution time.

| Reference | Minimum semantic contents | Input/output rule |
| --- | --- | --- |
| `SourceAssetRef` | `workspace_id`, `source_connection_id`, `source_asset_id`, and the stable asset `version_digest` | Materialize input only. It contains no credential value, connection string, raw path, or source row. |
| `SnapshotRef` | `workspace_id`, `session_id`, `dataset_id`, `snapshot_id`, committed manifest/content `version_digest`, schema fingerprint, and Snapshot contract version | A reference to one immutable committed Snapshot. It is readable only while committed and retained; a staging or tombstoned Snapshot is invalid. |
| `ArtifactRef` | `workspace_id`, `run_id`, `artifact_id`, typed artifact kind/version, content digest, and committed state | A reference to one immutable committed logical artifact. For a partitioned ExportArtifact it names the logical set; per-file digests remain in its manifest and are not extra Job outputs. |
| `VerificationBundleRef` | `workspace_id`, `run_id`, `bundle_id`, bundle/manifest digest and version, one accepted `SnapshotRef`, and its immutable member `ArtifactRef`s | A reference to one atomic verification publication. It is a typed bundle view, not a second Job/Run owner. |

Reference validation is fail-closed:

- all Workspace, Session, Run, Dataset, and PlanVersion relationships must
  agree with the Job and operation descriptor;
- a reference to another Workspace, another Run, an archived input, a missing
  object, an uncommitted object, a tombstone, or a digest mismatch is rejected;
- duplicate IDs or the same logical output represented by two reference kinds
  are rejected;
- references never contain secrets, raw cell values, full report bodies, or
  unbounded metadata.

### 3.3 Output reference cardinality

The following table freezes direct Job terminal outputs. Nested bundle members
are shown separately so a projection cannot count them twice.

| Operation | `SnapshotRef` output | direct `ArtifactRef` output | `VerificationBundleRef` output | Nested/manifest members |
| --- | ---: | ---: | ---: | --- |
| Materialize | exactly 1 | 0 | 0 | The Snapshot is the one materialized data output. |
| Verification | 0 | 0 direct | exactly 1 | The bundle contains exactly 1 accepted `SnapshotRef` equal to the input, exactly 1 ValidationReport `ArtifactRef`, exactly 1 DeduplicationReport `ArtifactRef`, and 0 or 1 RejectedRows `ArtifactRef` iff terminal rejections exist. |
| Profile | 0 | exactly 2 | 0 | Exactly 1 `profile_report.v1` and exactly 1 `quality_report.v1`; the quality report binds the profile digest. |
| Export | 0 | exactly 1 | 0 | Exactly 1 logical `ExportArtifact` `ArtifactRef`; its manifest may contain multiple ordered files/partitions but those are not extra Job outputs. |

The `VerificationBundleRef` is the direct result of Verification. If a storage
implementation also materializes a bundle-kind `ArtifactRefRecord`, that row
is the physical representation of the same bundle identity and must not be
returned as an additional direct output. The bundle's child report artifacts
remain distinct `ArtifactRef`s and are readable only through committed bundle
membership.

No operation may return a staged, failed, tombstoned, or merely planned output
reference in a successful or replayed Job result.

## 4. One lifecycle and one authority for all operations

### 4.1 Job and Run binding

Every accepted submission creates one E5 Job. A worker claim creates at most
one Run through the existing atomic `queued -> running` transition. The Run
copies the operation kind/version/digest and records the exact resolved input
references used by that attempt.

All four operations use the same E5-C0 Job/Run states and transitions:

```text
queued -> running -> succeeded
queued -> running -> failed
queued -> running -> cancelling -> cancelled
queued -> cancelling -> cancelled
queued -> failed
running -> cancelling -> failed
```

`cancelling -> succeeded` is forbidden. If terminal output publication wins
first, the Job/Run succeeds and a later cancellation observes that terminal
state. If cancellation wins first, no operation may publish a successful
output.

The operation is durable before admission. Worker-local enums, closures,
queues, or resolver state may accelerate execution but are caches only. A
fresh process must reconstruct the operation from the persisted Job and verify
its version, descriptor digest, references, and PlanVersion before it claims
or resumes work.

### 4.2 Shared cancellation, deadline, and run gate

All four operations receive the existing `RequestContext` cancellation and
deadline path and pass it to every connector, engine, profile, verification,
export, storage, and publication checkpoint. The existing engine concurrency
gate remains the sole active-run gate; no operation gets a private semaphore,
timeout state machine, or cancellation token authority.

The effective execution deadline is part of the typed execution policy and
therefore of `request_digest`. Existing E5-C0 bounds remain in force: the
normal execution deadline defaults to 15 minutes and may not exceed 30 minutes;
the existing engine and storage row/byte limits remain lower-level ceilings.
Operation-specific limits may be stricter but may never widen a lower-level
bound.

Cancellation is cooperative and must be checked at input, bounded batch,
operation phase, and pre-publication boundaries. A cancellation request is not
itself proof of cancellation. The durable `cancelled` terminal state is allowed
only after cleanup is confirmed, unless cancellation loses to a previously
committed terminal outcome.

### 4.3 Shared Event authority

All four operations use the same durable E5-E1 Event Stream and the two
existing stream kinds (`job` and `run`). There is no operation-specific event
store, progress log, or replay cursor.

Events may expose only bounded, sanitized operation metadata:

```text
operationKind, operationVersion, operationDigest,
phase, state, outputKinds, bounded counts, digests, failure category
```

`phase` is descriptive metadata on an existing Job/Run lifecycle event; it is
not a second state machine. The event type and event version remain governed by
E5-E1. Event sequence is assigned durably by the existing stream authority, is
monotonic per stream, and is not caller- or worker-chosen.

Events must not contain credentials, secret values, raw rows, full profile or
verification payloads, destination secrets, arbitrary plan code, backtraces,
or unsanitized connector errors. The existing 64 KiB event payload bound and
bounded replay/page rules remain in force. A subscriber disconnect never
cancels the Job or Run.

## 5. Operation contracts

### 5.1 Materialize

#### Typed input and bounds

Materialize accepts exactly:

- one published `PlanVersionRef` in the common Job envelope;
- one `SourceAssetRef` in `MaterializeV1`;
- one typed Materialize policy containing only bounds and options already
  admitted by the existing single-source Engine contract.

It accepts no `SnapshotRef`, `ArtifactRef`, `VerificationBundleRef`, raw
source rows, live Preview payload, SQL connector input, native DuckDB input,
or cross-source/join execution. The Phase-1 CSV, NDJSON, Parquet, Workbook,
and S3-compatible source capabilities remain governed by #3/#81 and their
existing connector contracts; SQL #9 and DuckDB #10 remain deferred.

The operation composes the existing connector read, Plan/Engine execution,
RequestContext, engine run gate, Snapshot bounds, and Snapshot publication
rules. A request that exceeds connector, Arrow, Engine, or Snapshot limits
fails before or at the bound with a typed sanitized failure. It does not
silently downgrade to Preview or change the Plan.

#### Terminal output and provenance

Success has exactly one committed `SnapshotRef` output and no ArtifactRef or
VerificationBundleRef. The Snapshot identity, Dataset identity, schema
fingerprint, input version digest, PlanVersion/digest, Engine contract/build,
Run identity, effective bounds, lineage, and caller-injected timestamps are
bound to the authoritative Run.

The SnapshotRef becomes readable only after the underlying Snapshot manifest,
partitions, digests, and control-plane visibility commit. A Run `snapshot_ref`
must not be set to a staged or uncommitted identity. The existing
`JobExecutionSpec.bundle_ref` resolver hook is not a Materialize output
authority under this contract; Verification owns VerificationBundle output.

Terminal failure, timeout, worker loss, or cancellation returns no SnapshotRef,
no ArtifactRef, and no VerificationBundleRef. Any staging residue is cleaned
by the existing storage recovery rules and cannot be inferred as a successful
output from files alone.

### 5.2 Verification

#### Typed input and bounds

Verification accepts exactly:

- one published PlanVersionRef in the common Job envelope;
- one committed `SnapshotRef` in `VerificationV1`;
- one typed verification policy bounded by the existing E4 validation and
  deduplication contract.

It does not accept a SourceAssetRef, another ArtifactRef, a live engine buffer,
Preview output, or a Snapshot that is staged, tombstoned, cross-workspace, or
whose manifest/content digest does not match the reference.

Verification reuses the existing E4 rule semantics, stable source-row
ordinal, exact dedup index, report schemas, rejected-row policy, cancellation
checkpoints, and report pack ceilings. It does not redefine Validate,
Deduplicate, finding severity, or raw-value handling. A bound, type,
integrity, rule, storage, cancellation, or deadline failure fails the whole
operation closed.

#### Bundle binding and terminal output

Verification succeeds only with exactly one committed `VerificationBundleRef`
bound to the current `run_id`, `job_id`, Workspace, Session, PlanVersion and
the exact input Snapshot ID and version digest. The bundle membership is:

1. exactly one accepted `SnapshotRef`, equal to the input SnapshotRef rather
   than a second Snapshot identity;
2. exactly one `ValidationReport` ArtifactRef, including zero-row reports when
   no validation finding exists;
3. exactly one `DeduplicationReport` ArtifactRef, including zero-row reports
   when no duplicate exists;
4. zero or one `RejectedRows` ArtifactRef, present exactly when terminal
   rejected rows exist.

The bundle ID, bundle artifact identity where physically represented, accepted
Snapshot identity, every member ArtifactRef identity, content digest, schema,
Run ID, input digest, PlanVersion/digest, E4 contract revision, and publication
timestamps are pairwise validated and written as one immutable membership
record. A child report is not independently visible outside the committed
bundle membership.

The publication sequence may stage the accepted Snapshot/report members and
their manifests, but the bundle, its member refs, and the Run terminal output
must become visible only at the one atomic bundle commit. Failed or cancelled
verification never returns a bundle or child reference and never exposes a
partial report, rejected payload, or staging path.

#### Provenance

Bundle provenance includes the source SnapshotRef and digest, Dataset/Session,
PlanVersion and canonical Plan digest, Run/Job identity, input schema
fingerprint, E4 contract/runtime identity, rule/dedup policy digest, bounded
row/byte counts, output member digests, and caller-injected lifecycle times.
It contains no raw cell value, credential, secret value, full connection
configuration, or arbitrary exception text.

### 5.3 Profile

#### Typed input and bounds

Profile accepts exactly:

- one published PlanVersionRef in the common Job envelope;
- one committed `SnapshotRef` in `ProfileV1`;
- one `ProfileRequestV1` as defined by ADR-003: target Snapshot, explicit
  ordered columns or all columns, `top_k`, and `histogram_buckets`.

Profile v1 is exact over its admitted scan scope and has no sampling parameter.
The existing ADR-003 ceilings apply: at most 1,048,576 scanned rows,
512 MiB scan bytes, 256 columns, `top_k` 100, 64 histogram buckets,
100,000 distinct entries per column, and 100,000 full-row distinct entries.
Requests outside those ceilings fail validation; binding at a scan ceiling is
reported as truncation according to ADR-003 and is not silently treated as a
complete scan.

Profile does not accept a live SourceAssetRef, Preview payload, ArtifactRef as
data input, VerificationBundleRef, SQL/DuckDB input, or an implicit process
local sample. Any later sampled profile must be a separately versioned
contract and cannot be smuggled into Profile v1.

#### Q-R1 -> Q-R2 composition law

Profile is one Job, one Run, one idempotency identity, one cancellation/deadline
path, and one Job/Run Event Stream. It composes:

```text
Q-R1 bounded streaming profile scan
        -> profile_report.v1 canonical artifact
        -> Q-R2 deterministic findings / quality report
```

Q-R2 consumes the exact bounded Q-R1 profile result or its staged canonical
report bytes. It must not reopen the source, perform a second data scan, create
a second Job or Run, acquire a second operation authority, or publish a second
terminal lifecycle. Q-R1 and Q-R2 share the same input SnapshotRef, Run ID,
PlanVersion/digest, profile request digest, scan bounds, and operation digest.

The Q-R1 profile report may be staged for Q-R2 consumption, but its
`ArtifactRef` is not visible to readers until the complete Profile output
publication commits. This prevents a Q-R2 failure from exposing a partial
Profile result.

#### Terminal output and provenance

Profile succeeds with exactly two committed `ArtifactRef`s:

1. one `profile_report.v1` containing the canonical DatasetProfile;
2. one `quality_report.v1` containing deterministic findings, the versioned
   QualityScore, completeness, missing-component semantics, and the exact
   digest of the profile report it consumed.

There is no SnapshotRef or VerificationBundleRef output. The two artifacts are
published as one logical operation result. Their `run_id`, Workspace, Session,
PlanVersion/digest, input SnapshotRef/digest, Profile/Quality contract
versions, detector identity, scan bounds, truncation flag, canonical content
digests, and lifecycle timestamps must agree. A quality report may not point to
a profile report from another Run or a different Snapshot digest.

The operation fails or is cancelled with zero visible output refs if either
Q-R1 scanning/canonicalization or Q-R2 deterministic analysis/publication
fails. It is successful only after both underlying artifact commits and the
single operation output transaction have completed.

Provenance follows ADR-003: it records the caller-supplied Run identity,
target SnapshotRef, resolved request/policy digest, profiling contract version,
Plan fingerprint where applicable, Q-R2 detector/quality versions, bounded
counts, and output digests. It contains no raw rows, retained cell values
beyond the subordinate contract's allowed bounded evidence, secrets, or
unsanitized errors.

### 5.4 Export

#### Typed input and bounds

Export accepts exactly:

- one published PlanVersionRef in the common Job envelope;
- one committed `SnapshotRef` in `ExportV1`;
- one typed `ExportRequestV1` governed by ADR-004.

The v1 format set is CSV, TSV, JSONL, or Parquet with the encoding, schema,
column order, row order, and null rules frozen by ADR-004. Instruction JSONL,
Chat JSONL, multi-Snapshot inputs, live engine buffers, Preview payloads,
Profile artifacts, and VerificationBundle artifacts are not Export v1 inputs.

The ADR-004 bounds remain hard ceilings: 10,000,000 rows, 8 GiB total output,
2 GiB per single file, 1,024 partitions, 16 GiB live staging bytes, and the
ADR-004 deadline/publication bounds. Export may be stricter but cannot widen
Snapshot, Arrow, Engine, storage, or API bounds.

#### Authoritative Run binding and terminal output

Export succeeds with exactly one committed logical `ExportArtifact` ArtifactRef
and no SnapshotRef or VerificationBundleRef output. The ExportArtifact's
`run_id` is exactly the current authoritative Run ID, not the Run that
originally created the source Snapshot and not a newly generated export-only
Run. Its manifest binds the current Run to:

- the exact source Snapshot ID, Dataset/Session, version/content digest and
  schema fingerprint;
- the exact Workspace, PlanVersion and canonical Plan digest recorded at Job
  submission;
- format and format-contract version, encoder/storage identity, row/byte/file
  counts, ordered per-file digests, and manifest digest;
- the registered destination-root reference and safe relative destination,
  with no secret-bearing configuration.

The physical export writer stages and verifies every byte, computes writer
digests, and commits the destination/manifest before the ArtifactRef becomes
readable or the Run can become successfully terminal. If a crash leaves final
files installed before the authoritative manifest/output transaction, those
files are unpublished recovery residue, not an ExportArtifact; recovery removes
them and no output ref is returned. Silent overwrite and partial destination
visibility are forbidden.

Failure, timeout, or cancellation returns zero visible ExportArtifact refs.
Staging and pre-publication residue is cleaned by the export recovery rules;
readers must never infer success from a file that lacks the committed
ArtifactRef/manifest binding.

#### Provenance

The ExportArtifact manifest records the source SnapshotRef, current Run ID,
Job/PlanVersion identity, format/encoder versions, bounds and totals, ordered
file/partition digests, destination-root reference, and lifecycle times. It
never records credential values, secret contents, raw connection strings,
arbitrary environment data, or unsanitized errors.

## 6. Versioning and fail-closed behavior

### 6.1 Operation and nested contract versions

`JobOperation v1` accepts exactly the four operation kinds in §2.1 with
`operation_version = 1`. The version is part of the durable Job identity and
request digest. It is not inferred from a route, a policy field, an artifact
kind, or a worker build.

The operation also records the subordinate contract versions needed to
interpret its descriptor and outputs:

- Materialize: the bound Plan, Engine, Snapshot, and connector contract/build
  identities;
- Verification: the E4 verification/bundle contract and report/artifact
  versions;
- Profile: `PROFILING_CONTRACT_VERSION`, `QUALITY_SCORE_VERSION`, and the
  `profile_report.v1`/`quality_report.v1` body versions;
- Export: the ADR-004 export/manifest/format and encoder versions.

Changing an input meaning, bound, output schema, digest rule, provenance rule,
or publication rule requires a new versioned contract or an explicit amendment;
it is not a silent v1 implementation detail.

### 6.2 Unknown and newer versions

Unknown, older-unsupported, or newer-than-supported operation kinds, operation
versions, nested contract versions, artifact body versions, or PlanVersion
formats fail closed. Best-effort interpretation, field skipping, coercion to
Materialize, or downgrade to a locally supported version is forbidden.

The failure behavior is deterministic:

- an unknown operation/version in a new request is rejected before Job creation
  with a typed sanitized validation error;
- an unknown operation/version in a queued durable Job prevents worker claim and
  produces one durable pre-run incompatible-operation failure, with no Run or
  output refs;
- an incompatible operation discovered while reconciling a non-terminal
  Job/Run fails closed with the existing worker-loss/incompatible-state
  recovery semantics and publishes no output;
- a corrupt or version-inconsistent terminal record remains immutable and is
  exposed only through a sanitized unavailable/integrity result, never by
  guessing a result from files or policy JSON.

Every failure path emits only the existing typed Job/Run failure/event shapes;
it does not create a second version-negotiation or error authority.

## 7. Atomic publication and reference visibility

The common publication law is:

```text
validate typed operation and refs
    -> stage bounded output and provenance
    -> verify content, digests, schema, lineage, and bounds
    -> commit underlying Snapshot/artifact/bundle bytes and manifests
    -> atomically attach terminal output refs to the authoritative Run/Job
    -> publish the terminal success event
```

The order is normative. No public output ref, successful terminal result, or
reader-visible artifact may exist before the underlying content and manifest
commit. A staged ref is not readable and is not a terminal output. A failed or
cancelled publication removes or recovers all uncommitted residue and leaves
zero visible partial outputs.

Operation-specific application of this rule is:

| Operation | Atomic visible set |
| --- | --- |
| Materialize | One committed Snapshot manifest/partitions plus one Run SnapshotRef. |
| Verification | One complete VerificationBundle, its accepted Snapshot membership and all present report members, plus one Run VerificationBundleRef. |
| Profile | The Q-R1 profile report and Q-R2 quality report together, plus both Run ArtifactRefs. |
| Export | One committed ExportArtifact manifest and its complete file/partition set, plus one Run ArtifactRef. |

Terminal Job/Run state and output references are compare-and-set durable
records. A reader never infers a terminal result from a file, an event alone,
or a non-terminal staging row. The first committed terminal transition wins;
later cancellation, duplicate submission, or restart reconciliation replays
that immutable result.

## 8. Workspace isolation, secrets, and public redaction

### 8.1 Workspace isolation

Every operation and every nested reference is checked against the Job
Workspace before execution and before every output read/write. A reference,
cursor, artifact handle, Snapshot, bundle member, PlanVersion, or destination
root from another Workspace fails closed with the same non-disclosing not-found
or unauthorized policy selected by the security boundary. Operation identity
never bypasses object ownership.

Export destinations remain inside a registered allowed root as defined by
ADR-004. Verification/Profile readers accept only committed members of the
current operation's bound input. No worker may substitute a path, object ID,
or storage handle based on process-local state.

### 8.2 Secret-reference-only boundary

`SourceAssetRef` and all connection-related provenance carry only stable
identifiers and secret references. They never carry credential values, access
tokens, raw connection strings, or provider configuration containing secrets.
The typed operation descriptor, Job/Run rows, Event payloads, error summaries,
artifact manifests, profile/quality reports, verification reports, and export
manifests are all secret-free surfaces.

Raw source values may exist only in the bounded execution buffers and the
specific committed data/report payloads allowed by the existing E4/Q/X
contracts. They must not enter operation identity, event metadata, public
errors, logs, debug output, or ordinary Job/Run projections.

### 8.3 Public errors and metadata

Public responses and events expose stable typed categories, operation kind and
version where safe, object IDs, digests, bounded counts, and sanitized
provenance. They do not expose raw exception strings, filesystem internals,
credentials, source rows, full validation payloads, profile bodies, or secret
sentinels. A lower layer may retain diagnostic detail internally, but the
transport-neutral API boundary receives only the repository's sanitized error
envelope.

## 9. Compatibility with E5-A1 and E5-E1

### 9.1 E5-A1 transport-neutral API boundary

`JobOperationV1` is a domain request/response type at the E5-A1 boundary. The
existing generic Job operations remain the semantic entry points:

- `job.submit` accepts one typed `JobOperation` plus the common Job envelope
  and idempotency key;
- `job.read` and `job.list` project the persisted operation kind/version,
  operation digest, lifecycle state, and typed terminal refs;
- `job.cancel` uses the common durable cancellation path;
- Run and Artifact reads use the existing bounded identity and Workspace
  checks.

No operation-specific HTTP route, handler-defined state, policy-only operation
switch, or second request/response envelope is introduced by this contract.
The API manifest and OpenAPI/schema version handshake remain E5-A1 authority;
an API consumer or server that does not support the required operation/schema
version fails closed before execution. Transport names and HTTP status choices
may map these typed results, but cannot change their meaning.

### 9.2 E5-E1 Event Stream

E5-E1 remains the only live/replayable Job/Run Event authority. Event frames
may project the persisted operation kind/version/digest, phase, output kinds,
and sanitized terminal references under the existing event version and payload
bound. Resume cursors, sequence assignment, pagination, replay, slow-consumer
handling, and terminal-event/state agreement remain unchanged.

Profile Q-R1 and Q-R2 phase information is carried within the same Run stream;
it does not create a profile stream or a second terminal event authority.
Verification child-artifact events are likewise emitted on the owning Run
stream and do not create an independent bundle stream.

## 10. Restart, recovery, and idempotent replay

Restart recovery reads durable Jobs, typed operation descriptors, Run records,
input references, output references, and event sequence from storage. It does
not rely on an in-memory operation enum, resolver closure, queued future,
profile accumulator, Q-R2 state, export writer object, or worker-local output
map.

The shared recovery rules are:

| Durable state at restart | Recovery |
| --- | --- |
| Queued Job with supported v1 operation | Remains the same queued Job with the same identity, operation digest, inputs, PlanVersion, and event sequence. |
| Queued Job with unknown/invalid operation | Fails closed before Run creation with one incompatible-operation failure; no output refs. |
| Running Job/Run | Reconciles through the E5-C0 worker-loss rule; no operation-specific retry or new Run is created. |
| Cancelling Job/Run | Preserves a previously committed terminal cancellation; otherwise uses the shared cleanup/failure rule and never claims cleanup that was not observed. |
| Staged Snapshot, report, bundle, or export without committed visibility | Storage recovery removes unpublished residue; no output ref or success is inferred. |
| Committed output with a durable Run binding | Remains readable and is replayed as the existing authoritative result. |
| Terminal Job/Run | State, operation identity, refs, failure, and event history remain immutable. |

Recovery is idempotent. Re-running it cannot create a second Run, rescan a
Profile input, emit a second terminal result, republish a partial bundle, or
turn an installed-but-uncommitted Export file into a visible ArtifactRef.

## 11. Minimum later E5-J2 bridge boundary

This section defines acceptance for a later implementation task; it is not an
implementation commitment in this docs-only PR.

The later E5-J2 bridge is minimally responsible for:

1. durable Job/Run storage of operation kind, operation version, validated
   descriptor/digest, and the operation-specific terminal reference set;
2. one typed submission/claim path that validates all four operation variants
   and dispatches them through the existing JobRuntime, Engine gate,
   RequestContext, storage, and Event authority;
3. Materialize -> one SnapshotRef;
4. Verification -> one atomically committed VerificationBundleRef with the
   exact E4 member cardinality;
5. Profile -> one Run composing Q-R1 then Q-R2 with exactly one data scan and
   two atomically published report ArtifactRefs;
6. Export -> one ExportArtifact ArtifactRef bound to the authoritative Run and
   committed only after the underlying export manifest/files are durable;
7. restart, cancellation, deadline, duplicate, unknown-version, cross-
   Workspace, digest-mismatch, storage-failure, and no-partial-publication
   behavior under the common E5-C0/E5-E1 rules;
8. typed E5-A1 projections and E5-E1 metadata only after the relevant API/Event
   implementation gates authorize those changes.

The bridge must not:

- implement a second Job/Run/Event/queue/retry state machine;
- encode operation meaning only in policy JSON or infer it from output files;
- run Q-R1 and Q-R2 as separate Jobs, Runs, idempotency identities, or source
  scans;
- make a staged ref readable before its underlying commit;
- add SQL/DuckDB, SEC/AUD/AUT/OPS, frontend, AI execution, HTTP listener, or
  unrelated product capabilities;
- silently broaden the Phase-1 source matrix, bounds, retention model,
  authorization model, or public error taxonomy;
- reopen #232 before this contract and the later bridge have their separate
  exact-head acceptance and rebind evidence.

If implementing this boundary requires a new product capability not already
covered by E5-C0, E4, ADR-003, ADR-004, E5-A1, or E5-E1, the bridge must stop
and raise a separately scoped contract/roadmap decision. It must not widen
E5-J2 through an implementation PR.

## 12. Explicitly deferred product surfaces

This contract does not activate the following work:

- E5-G1 / [#232](https://github.com/X44421/stillflow/issues/232) remains
  `E5_G1_SCOPE_BLOCKED` until the operation bridge is independently accepted,
  the Issue is rebound to the then-current exact `main`, and the gate is
  separately dispatched;
- Q-A1 full Profile/Quality/Drift API, including profile history, baseline
  selection, drift comparison, disposition, and history/retention queries;
- X-A1 Export Job/API retention and delete operations, destination lifecycle,
  and any broader export management surface;
- Q-D1 history/drift runtime wiring, SEC Workspace authorization/credential
  lifecycle, AUD audit query/export, AUT scheduling, OPS service/retention/GC,
  SQL connector #9, and native DuckDB #10;
- any frontend, Desktop/CLI product surface, or transport/listener packaging.

Profile v1 may publish the Q-R1/Q-R2 artifacts defined here, but it does not
create history or Drift objects. Export v1 may publish an ExportArtifact, but
it does not define retention/delete API semantics; those remain the X-A1 and
OPS/retention lines.

## 13. Acceptance matrix and non-implementation gate

An independent reviewer must be able to derive the following from this single
document and the exact PR head:

| Acceptance fact | Frozen by |
| --- | --- |
| Typed/versioned closed operation union and fail-closed unknown handling | §§2, 6 |
| Operation participates in idempotency with complete request inputs | §3.1 |
| Typed reference vocabulary, Workspace binding, and output cardinality | §§3.2, 3.3 |
| One E5 Job/Run lifecycle, cancellation/deadline path, and Event authority | §4 |
| Materialize input, one SnapshotRef output, and provenance | §5.1 |
| Verification input, bundle/member binding, cardinality, and provenance | §5.2 |
| Profile Q-R1 -> Q-R2 one-Job/one-Run/one-scan law and two outputs | §5.3 |
| Export input, bounds, authoritative Run binding, and one output ArtifactRef | §5.4 |
| Atomic publication: no visible ref before underlying commit | §7 |
| Restart from durable operation and no process-local authority | §§4.1, 10 |
| Workspace isolation, secret-reference-only, and public redaction | §8 |
| E5-A1/E5-E1 compatibility and deferred product boundaries | §§9, 12 |
| Smallest later E5-J2 bridge and explicit prohibitions | §11 |

The docs-only delivery gate is:

- the diff contains exactly this new Markdown file under `docs/issues/`;
- no Rust, TypeScript, frontend, API/manifest, Cargo, dependency, workflow,
  coordination implementation, or deferred product surface changes;
- `git diff --check` is clean;
- all relative document links resolve at the entry base;
- no operation semantics are left to an untyped policy, a transport route, or
  process-local state;
- no independent acceptance, Ready, merge, or Issue close is claimed by this
  task.

After this document is complete and its exact-head CI is green, the only
permitted terminal handoff is:

```text
READY_FOR_E5_J2_C0_INDEPENDENT_REVIEW
```

The handoff is a request for a separate reviewer/workspace to audit the exact
checkout, contract, diff, canonical checks, CI, and scope proof. It is not an
acceptance or merge decision.

## 14. References and consequences

- [#3 Phase 1 scope authority](https://github.com/X44421/stillflow/issues/3)
- [#81 roadmap/dependency authority](https://github.com/X44421/stillflow/issues/81)
- [#232 E5-G1 scope-blocked gate](https://github.com/X44421/stillflow/issues/232)
- [E5-C0 unified control-plane contract](issue-191-unified-control-plane-contract.md)
- [E4 validation/rejected-rows contract](issue-054-validation-rejected-rows-contract.md)
- [ADR-003 profiling, quality, and findings](../architecture/adr-003-profiling-quality-and-findings.md)
- [ADR-004 export and output artifacts](../architecture/adr-004-export-and-output-artifacts.md)
- [E5 runtime domain inventory](e5-runtime-domain-inventory.md), as historical
  discovery evidence only
- [Storage publication/recovery inventory](storage-publication-recovery-inventory.md)

The consequence of this freeze is deliberately narrow: later runtime work may
add one durable operation discriminator and dispatch existing domain runtimes,
but it may not let the bridge become a second product architecture. The
operation, its version, its refs, its provenance, its terminal state, and its
visibility point all remain part of the one E5 Job/Run authority.
