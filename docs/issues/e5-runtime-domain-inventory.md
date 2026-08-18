# E5-D0 Runtime Domain Inventory

> Status: discovery-only, non-binding; inventory complete for the stated base.
>
> Issue: #58
>
> Inventory base: `main@85502cbebb1fab461fe42d30fe019ad20613aa7c`
>
> Delivery: docs-only
>
> E5 contract status: not frozen

## 1. Scope and methodology

This document records repository facts observed at:

`main@85502cbebb1fab461fe42d30fe019ad20613aa7c`

It is an input to a future E5-C0 contract. It does not freeze E5 public
fields, lifecycle semantics, serialization formats, HTTP endpoints, crate
dependencies, or runtime implementation.

PR #53 and PR #57 are read-only references for this inventory. They are not
merged, rebased, or cherry-picked into this branch.

Repository findings in this document use the following implementation-status
vocabulary:

- `implemented`: a concrete definition and/or executable behavior exists on the
  inventory base.
- `placeholder`: a name, crate, type, or architectural slot exists, but the
  intended capability is not implemented.
- `missing`: no corresponding E5 domain capability is implemented on the
  inventory base.
- `blocked by E4`: the decision depends on E4 contracts or implementation that
  are not part of the inventory base.

Persistence is classified separately:

- `persisted`: concrete persistence write/read behavior exists.
- `runtime-only`: the value exists only during execution.
- `defined-but-not-persisted`: a domain definition exists, but no persistence
  path has been established by repository evidence.
- `unknown`: the inspected evidence is insufficient to classify persistence.

Serialization support alone is not considered evidence of persistence.

For each repository claim, this inventory records an exact repository path and
the inventory base SHA.

## 2. Current object inventory

### 2.1 Session

- **Definition:** `backend/crates/stillflow-core/src/domain/session.rs`
- **Status:** `implemented` as a core domain type.
- **Fields:**
  - `id: Uuid`
  - `connection_ids: Vec<Uuid>`
  - `created_at: DateTime<Utc>`
  - `updated_at: DateTime<Utc>`
- **Behavior:** `Session::new`, `with_connection`, `add_connection`, and
  `primary_connection_id`; `Serialize`/`Deserialize` with `camelCase`.
- **Persistence:** `defined-but-not-persisted`. No SQLite table, repository, or
  control-plane store writes `Session` on the inventory base.
- **Notes:** `session_id` is already a foreign key concept in the storage
  snapshot schema (`backend/crates/stillflow-storage/src/store.rs`), but no
  `sessions` table exists.

### 2.2 Dataset

- **Definition:** `backend/crates/stillflow-core/src/domain/dataset.rs`
- **Status:** `implemented` as a core domain type.
- **Fields:**
  - `id: Uuid`
  - `session_id: Uuid`
  - `source_asset_id: Uuid`
  - `name: String`
  - `created_at: DateTime<Utc>`
- **Behavior:** `Dataset::new`; `Serialize`/`Deserialize` with `camelCase`.
- **Persistence:** `defined-but-not-persisted`. No SQLite table or repository
  stores `Dataset` on the inventory base.
- **Notes:** `dataset_id` is referenced by the storage snapshot schema
  (`snapshots.dataset_id`), but no `datasets` table exists.

### 2.3 DatasetSnapshot

- **Definition:** `backend/crates/stillflow-core/src/domain/snapshot.rs`
- **Status:** `implemented` as a stable core domain type.
- **Fields (private, validated by constructors):**
  - `version: u16`
  - `id: Uuid`
  - `dataset_id: Uuid`
  - `session_id: Uuid`
  - `source_asset_id: Uuid`
  - `schema: LogicalSchema`
  - `schema_fingerprint: LogicalSchemaFingerprint`
  - `stats: SnapshotStats` (`row_count: u64`, `stored_byte_count: u64`,
    `partition_count: u32`)
  - `lineage: BTreeSet<Uuid>`
  - `quality_score: Option<u8>`
  - `created_at: DateTime<Utc>`
- **Behavior:** `try_new` / `try_from_parts` validate version, nil identities,
  quality score, stats consistency, schema validity, and fingerprint match;
  `Serialize`/`Deserialize` use a private `DatasetSnapshotData` with
  `camelCase`.
- **Persistence:** `persisted`. The storage crate writes the descriptor into
  SQLite columns in `snapshots` (`backend/crates/stillflow-storage/src/store.rs`)
  and publishes payload as immutable Parquet partitions.
- **Notes:** serialized JSON deliberately excludes `storageRef`/`schemaFields`/
  `snap://` payload pointers; storage references are external to the core type.

### 2.4 SnapshotManifest

- **Definition:** `backend/crates/stillflow-storage/src/manifest.rs`
- **Status:** `implemented` in `stillflow-storage`.
- **Fields:**
  - `snapshot: DatasetSnapshot`
  - `partitions: Vec<SnapshotPartition>`
- **SnapshotPartition fields:**
  - `sequence: u32`
  - `row_count: u64`
  - `stored_byte_count: u64`
  - `digest: ContentDigest`
- **Behavior:** `SnapshotManifest::try_new` validates partition count, contiguous
  sequences, and row/byte totals against `DatasetSnapshot` stats.
- **Persistence:** `persisted` as derived state. The manifest itself is not
  stored as a JSON blob; its components are persisted in the SQLite
  `snapshots`/`partitions` tables and the Parquet file digests.
- **Notes:** `SnapshotManifest` is returned by `SnapshotStore::load_manifest`,
  `verify_snapshot`, and `SnapshotWriter::commit`.

### 2.5 IngestionEvent

- **Definition:** `backend/crates/stillflow-core/src/events/mod.rs`
- **Status:** `implemented` as a core auditable event type.
- **Fields:**
  - `id: Uuid`
  - `session_id: Uuid`
  - `object_kind: ObjectKind`
  - `object_id: Uuid`
  - `relationship: RelationshipKind`
  - `timestamp: DateTime<Utc>`
  - `metadata: serde_json::Value`
  - `error: Option<SanitizedErrorSummary>`
- **Behavior:** `IngestionEvent::try_new` validates metadata through
  `ensure_safe_event_metadata`; `Serialize`/`Deserialize` revalidates on read.
- **Persistence:** `defined-but-not-persisted`. No event repository or SQLite
  event table exists on the inventory base.
- **Notes:** `ObjectKind` already includes `Session`, `SourceConnection`,
  `SourceAsset`, `Dataset`, `Snapshot`, and `Capability`, but there is no
  generic Job/Run event log.

### 2.6 RequestContext

- **Definition:** `backend/crates/stillflow-core/src/request/mod.rs`
- **Status:** `implemented` as a core runtime control type.
- **Fields:**
  - `cancellation: CancellationToken`
  - `deadline: Option<Instant>`
- **Behavior:** carries cancellation and deadline through connector and engine
  calls; `ensure_active` returns `Cancelled`/`Timeout` errors.
- **Persistence:** `runtime-only`. It is not serializable and has no storage
  representation.
- **Notes:** This is the current cancellation/deadline propagation mechanism,
  not a Job/Run state machine.

### 2.7 ExecutionIdentities

- **Definition:** `backend/crates/stillflow-engine/src/lib.rs`
- **Status:** `implemented` in `stillflow-engine`.
- **Fields:**
  - `snapshot_id: Uuid`
  - `dataset_id: Uuid`
  - `session_id: Uuid`
  - `created_at: DateTime<Utc>`
  - `started_at: DateTime<Utc>`
  - `lineage: BTreeSet<Uuid>`
  - `quality_score: Option<u8>`
- **Behavior:** consumed by `ExecutionRequest` and passed into
  `SnapshotDraft::try_new` and `SnapshotStore::begin_snapshot`.
- **Persistence:** `runtime-only`. It is an engine input, not a stored object.
- **Notes:** This is the existing caller-injected identity bundle for materialize
  runs.

## 3. Missing object matrix

| Object | Status on `main@85502cb` | Evidence |
| --- | --- | --- |
| `Job` | `missing` | No `Job` type, repository, table, or state machine exists in `backend/crates` |
| `Run` | `missing` | No `Run` type or run record exists; `ExecutionEngine::materialize` is a direct async call, not a persisted run |
| generic `Event` (Job/Run event log) | `missing` | Only `IngestionEvent` exists; no event stream, sequence, retention, or Job/Run event repository |
| `Artifact` | `missing` | No artifact type, artifact reference, ownership handle, or bounded artifact reader exists |
| `ArtifactRef` | `missing` | Storage returns `SnapshotManifest`/`SnapshotBatchReader`, but there is no generic `ArtifactRef` domain object |
| `SourceConnection` persistence | `missing` | Type exists in core but no repository/table stores connections |
| `SourceAsset` persistence | `missing` | Type exists in core but no repository/table stores discovered assets |
| `Dataset` persistence | `missing` | Type exists in core but no repository/table stores datasets |
| `Session` persistence | `missing` | Type exists in core but no repository/table stores sessions |
| `DatasetSnapshot` persistence | `implemented` | Stored by `stillflow-storage` in SQLite + Parquet |
| `SnapshotManifest` persistence | `implemented` | Derived from SQLite metadata and Parquet partition digests |

## 4. Crate ownership

### 4.1 Accepted dependency direction

```text
stillflow-api
    -> stillflow-engine
        -> stillflow-plan
        -> stillflow-connectors
        -> stillflow-storage
            -> stillflow-core
```

### 4.2 Current ownership

| Type | Current owner crate | Evidence |
| --- | --- | --- |
| `Session` | `stillflow-core` | `backend/crates/stillflow-core/src/domain/session.rs` |
| `Dataset` | `stillflow-core` | `backend/crates/stillflow-core/src/domain/dataset.rs` |
| `DatasetSnapshot` | `stillflow-core` | `backend/crates/stillflow-core/src/domain/snapshot.rs` |
| `SnapshotManifest` | `stillflow-storage` | `backend/crates/stillflow-storage/src/manifest.rs` |
| `IngestionEvent` | `stillflow-core` | `backend/crates/stillflow-core/src/events/mod.rs` |
| `RequestContext` | `stillflow-core` | `backend/crates/stillflow-core/src/request/mod.rs` |
| `ExecutionIdentities` | `stillflow-engine` | `backend/crates/stillflow-engine/src/lib.rs` |
| `SourceConnection` | `stillflow-core` | `backend/crates/stillflow-core/src/domain/connection.rs` |
| `SourceAsset` | `stillflow-core` | `backend/crates/stillflow-core/src/domain/asset.rs` |

### 4.3 Non-binding ownership candidates for E5

These are candidates only; they do not freeze E5 ownership.

| Missing capability | Candidate owner | Rationale preserving dependency direction |
| --- | --- | --- |
| `Job`, `Run`, generic `Event`, `Artifact` domain values | `stillflow-core` | Stable runtime domain contracts belong in the lowest layer; `stillflow-api` and `stillflow-engine` can depend on them |
| Job/Run/Event/Artifact repositories and migrations | `stillflow-storage` | Control-plane SQLite persistence is already owned by `stillflow-storage` |
| Job execution and cancellation propagation | `stillflow-engine` | The engine already owns `ExecutionEngine`, run gating, and `RequestContext` propagation |
| HTTP operations for Job/Run/Status/Cancel/Artifact read | `stillflow-api` | API layer translates external requests into engine/storage calls and owns no domain semantics |

## 5. Existing source, asset, and dataset persistence facts

- `SourceConnection` and `SourceAsset` are constructed per call and passed
  through `ConnectorRegistry` methods
  (`backend/crates/stillflow-connectors/src/registry.rs`). No repository writes
  them to SQLite.
- `Session` and `Dataset` exist as core types but are not written by any crate
  on the inventory base.
- `stillflow-storage` owns the only SQLite persistence:
  `backend/crates/stillflow-storage/src/store.rs` creates `publications`,
  `snapshots`, and `partitions` tables in schema version 1.
- The `snapshots` table persists snapshot descriptor fields
  (`dataset_id`, `session_id`, `source_asset_id`, `schema_json`,
  `schema_fingerprint`, `row_count`, `stored_byte_count`, `partition_count`,
  `lineage_json`, `quality_score`, `created_at_utc`, `state`,
  `tombstoned_at_utc`).
- The `partitions` table persists per-partition `sequence`, `row_count`,
  `stored_byte_count`, and `sha256`.
- Immutable tabular payload is stored as Parquet under a managed root;
  `SnapshotStore::read_batches` returns a bounded `SnapshotBatchReader`
  (`backend/crates/stillflow-storage/src/store.rs`).
- `SnapshotStore::recover` and `collect_garbage` exist for publication recovery
  and maintenance (`backend/crates/stillflow-storage/src/store.rs`).

## 6. E5 decision inputs

The following are inputs to E5-C0, not frozen decisions.

### 6.1 ID and clock injection

- Current evidence: `Session`, `Dataset`, `SourceConnection`, and `SourceAsset`
  generate IDs/timestamps internally via `Uuid::new_v4()` and `Utc::now()`
  in their `new`/`try_new` constructors.
- `ExecutionIdentities` already provides caller-injected IDs and timestamps for
  snapshot materialization.
- E5 decision: whether `Session`/`Job`/`Run`/`Event`/`Artifact` IDs and clocks
  must also be caller-injected or remain constructor-generated.

### 6.2 Idempotency key

- Current evidence: no idempotency key field or replay mechanism exists.
- `SnapshotStore::begin_snapshot` inserts a publication row and aborts on
  failure, but there is no Job/Run-level idempotency contract.
- E5 decision: idempotency-key scope, replay result, and uniqueness constraint.

### 6.3 State machine and legal transitions

- Current evidence: `ExecutionEngine::materialize` is a single blocking call
  with a semaphore run gate; there is no `Queued`/`Running`/`Cancelling`
  state model.
- `RequestContext` provides cancellation/deadline at the call level.
- E5 decision: `Session -> Job -> Run -> Event -> Artifact` cardinality, state
  values, and a total transition table.

### 6.4 Restart recovery

- Current evidence: `SnapshotStore` has crash recovery for snapshot
  publication, but no control-plane Job/Run recovery.
- E5 decision: how `Queued`/`Running`/`Cancelling` runs are reconciled after a
  fresh-process restart.

### 6.5 Retention

- Current evidence: `SnapshotStore::tombstone_snapshot` and
  `collect_garbage` exist for snapshot lifecycle; no event retention or
  artifact retention policy exists.
- E5 decision: event sequence/order, redaction, retention, and artifact
  ownership/read handles.

### 6.6 Concurrency and execution limits

- Current evidence: `MAX_ENGINE_CONCURRENT_RUNS = 4` and a semaphore gate in
  `stillflow-engine`; storage has bounded active readers/publishers.
- E5 decision: queue bounds, run concurrency, cancellation/deadline
  propagation, and resource caps.

### 6.7 Event redaction and secrets

- Current evidence: `IngestionEvent` validates metadata through
  `ensure_safe_event_metadata`; `SourceConnection` rejects secret fields and
  stores only `CredentialRef`.
- E5 decision: which Job/Run event fields are redacted and how secrets stay out
  of events and artifact metadata.

### 6.8 Artifact references and read handles

- Current evidence: `SnapshotStore` returns `SnapshotManifest` and
  `SnapshotBatchReader` for snapshot data; there is no generic `ArtifactRef`.
- E5 decision: artifact ownership, read handle bounds, and publication rules
  for Verification/Quality/Export artifacts.

### 6.9 Preview provenance without payload

- Current evidence: connector `PreviewData` is ephemeral and validated in
  memory (`backend/crates/stillflow-core/src/domain/preview.rs`); it is not
  persisted on the inventory base.
- PR #53 (read-only reference) adds a bounded node-level Preview runtime but is
  not merged into `main@85502cb`.
- E5 decision: if Preview records only provenance (identities, limits, result
  metadata) and never persists payload, the contract must say so explicitly.

## 7. Capability inventory

The following are current capabilities on `main@85502cb`, not endpoint designs.

| Capability | Status | Evidence |
| --- | --- | --- |
| Source connection test | `implemented` (connector layer) | `ConnectorRegistry::test_connection` in `backend/crates/stillflow-connectors/src/registry.rs` |
| Asset discovery | `implemented` (connector layer) | `ConnectorRegistry::discover` |
| Asset inspection | `implemented` (connector layer) | `ConnectorRegistry::inspect` |
| Connector Preview | `implemented` (connector layer) | `ConnectorRegistry::preview` and `PreviewData` in core |
| Streaming read | `implemented` (connector layer) | `ConnectorRegistry::read_batches` and `BatchStream` |
| Checkpoint/resume | `implemented` (connector layer) | `ConnectorRegistry::checkpoint` |
| Engine preflight | `implemented` | `ExecutionEngine::preflight` |
| Engine materialize / Run | `implemented` as a direct async call | `ExecutionEngine::materialize` |
| Bounded node-level Preview runtime | `missing` on inventory base | Not present on `main@85502cb`; read-only reference is PR #53 |
| Job/Run Status | `missing` | No Job/Run types or API |
| Cancel | `missing` at Job level; call-level cancellation exists | `RequestContext` cancellation is implemented; no Job cancel endpoint/state |
| Artifact read | `missing` as generic API; snapshot read exists | `SnapshotStore::read_batches` is storage-level, not an E5 Artifact API |
| HTTP/Axum API | `missing` | `backend/crates/stillflow-api/src/lib.rs` is a placeholder with only `crate_name()` |
