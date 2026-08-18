# X0-D0 Export / Output Artifact Fact Inventory

> Issue: #66
>
> Delivery: docs-only; the only changed file is `docs/issues/export-output-artifact-inventory.md`.
>
> Inventory base: `main@89aab2551b8f73a32ed575bf75b3e3866b39d37c`
>
> Branch: `agent/issue-066-export-output-inventory`
>
> Status: facts-only; no X-C0 contract is frozen and no export runtime or API is authorized.

All repository paths in this document are relative to the repository root at the
full inventory base SHA `89aab2551b8f73a32ed575bf75b3e3866b39d37c`. Every fact
row is bound to that base; rows that mention unmerged proposals are explicitly
labeled `proposed / unmerged`.

## 1. Backend output-symbol inventory

This section records symbols that are related to writing, serializing,
downloading, or exporting output. It separates:

- final user-facing export runtime;
- internal Snapshot/Parquet publication;
- ingestion readers and parsers;
- connector byte-layer upload capability;
- test-only writers;
- placeholder API surfaces.

| Symbol | Exact path | Crate | Current signature / fields | Writes final user-facing output? | Behavior and bounds | Persistence / publication status | Evidence base |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `stillflow-api::crate_name` | `backend/crates/stillflow-api/src/lib.rs` | `stillflow-api` | `pub fn crate_name() -> &'static str` | No | Placeholder API crate; no Axum routes, request types, download or export endpoints exist. | Not persisted; API surface is not implemented. | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| `ExecutionEngine::materialize` | `backend/crates/stillflow-engine/src/engine.rs` | `stillflow-engine` | `pub async fn materialize(&self, request: ExecutionRequest<'_>) -> Result<SnapshotManifest, EngineError>` | No; writes an internal immutable Snapshot | Reads connector batches, transforms, appends to `SnapshotWriter`, and commits a `SnapshotManifest`. It is the current run path, not an export path. | Internal Snapshot persisted by `stillflow-storage`; no export artifact is created. | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| `SnapshotWriter::append` / `SnapshotWriter::commit` | `backend/crates/stillflow-storage/src/store.rs` | `stillflow-storage` | `pub fn append(&mut self, envelope: &BatchEnvelope) -> Result<(), StorageError>`; `pub fn commit(self) -> Result<SnapshotManifest, StorageError>` | No; internal Snapshot partition writer | Writes staged Parquet with `create_new`, installs final Parquet via rename, and commits SQLite snapshot/partition rows atomically. Enforces envelope/partition/row/byte limits. | Persisted as immutable Snapshot partitions plus SQLite manifest. | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| `SnapshotStore::load_manifest` | `backend/crates/stillflow-storage/src/store.rs` | `stillflow-storage` | `pub fn load_manifest(&self, snapshot_id: Uuid) -> Result<SnapshotManifest, StorageError>` | No; read-only Snapshot metadata access | Loads a visible snapshot descriptor and partition rows, revalidates schema fingerprint and totals. | Reads persisted SQLite metadata. | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| `SnapshotStore::read_batches` / `SnapshotBatchReader` | `backend/crates/stillflow-storage/src/store.rs` | `stillflow-storage` | `pub fn read_batches(&self, snapshot_id: Uuid) -> Result<SnapshotBatchReader, StorageError>`; `impl Iterator for SnapshotBatchReader` | No; internal Snapshot read handle | Returns one `BatchEnvelope` per Parquet partition in manifest order; verifies length, SHA-256 digest, Parquet schema, row count, and single-batch shape before returning. | Reads persisted immutable Parquet partitions. | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| `SnapshotStore::verify_snapshot` | `backend/crates/stillflow-storage/src/store.rs` | `stillflow-storage` | `pub fn verify_snapshot(&self, snapshot_id: Uuid) -> Result<SnapshotManifest, StorageError>` | No | Reads every partition through the same integrity checks as `read_partition`. | Reads persisted Snapshot; no output artifact is produced. | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| `ArrowWriter` in `write_partition` | `backend/crates/stillflow-storage/src/store.rs` | `stillflow-storage` | `ArrowWriter::try_new(file, envelope.payload().schema(), Some(properties))` | No; internal Parquet Snapshot encoding | Encodes one non-empty `BatchEnvelope` into one Parquet file with Snappy compression and `MAX_BATCH_ROWS` row-group target. | Persisted as immutable Snapshot partition. | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| `ContentDigest` / `digest_file` | `backend/crates/stillflow-storage/src/digest.rs` | `stillflow-storage` | `pub struct ContentDigest([u8; 32])`; `pub(crate) fn digest_file(file: &mut File) -> Result<ContentDigest, StorageError>` | No; internal partition digest | SHA-256 over the complete finalized Parquet file bytes, read sequentially with a 64 KiB buffer. | Stored in SQLite `partitions.sha256`; not an export digest. | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| `serde_json::to_writer` in `read.rs` | `backend/crates/stillflow-connector-local-tabular/src/read.rs` | `stillflow-connector-local-tabular` | Used when encoding projected JSON rows for Polars JSON-lines parsing | No; internal decode path | Re-encodes validated JSON objects into an in-memory byte buffer so Polars can parse them as JSON Lines. It does not write a file or response body. | Runtime-only internal transformation. | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| `serde_json::to_string` / `to_vec` serialization | `backend/crates/stillflow-core/src/domain/snapshot.rs`, `backend/crates/stillflow-core/src/batch.rs`, `backend/crates/stillflow-plan/src/plan.rs` | `stillflow-core` / `stillflow-plan` | Serialize `DatasetSnapshot`, `LogicalSchema`, `LogicalPlan`, and related values | No | Serialization of domain/plan values; no file or response-body export path. | Used for in-memory/JSON validation and metadata, not final export. | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| `ObjectStorageAccess::upload` | `backend/crates/stillflow-connector-object-store/src/access.rs` | `stillflow-connector-object-store` | `async fn upload(&self, key: &str, body: ObjectByteStream, context: &RequestContext) -> ConnectorResult<ObjectInfo>` | No product export today; it is a connector byte-layer write capability | Streams bytes through multipart upload, enforces `MAX_UPLOAD_CHUNKS` and `max_object_bytes`, aborts on source error/cancellation, and returns uploaded `ObjectInfo`. | Capability exists in `ObjectStorageAccess`; current call sites are tests and server-side composition via `open_access`, not a SourceConnector or product Export API. | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| `ObjectStoreConnector::open_access` | `backend/crates/stillflow-connector-object-store/src/lib.rs` | `stillflow-connector-object-store` | `pub async fn open_access(&self, connection: &SourceConnection, context: &RequestContext) -> ConnectorResult<Arc<dyn ObjectStorageAccess>>` | No | Opens the provider-neutral byte layer and returns an `ObjectStorageAccess` that includes `upload`. This is a server-side composition seam, not a final export runtime. | Runtime-only access object; no export metadata is persisted. | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| `LocalTabularConnector` | `backend/crates/stillflow-connector-local-tabular/src/lib.rs` | `stillflow-connector-local-tabular` | `#[async_trait] impl SourceConnector for LocalTabularConnector` | No | Implements discovery, inspection, preview, and read for CSV/TSV/JSON/NDJSON/Parquet. There is no writer or export method in the connector trait or implementation. | Read-only ingestion connector. | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| `ObjectStoreConnector` | `backend/crates/stillflow-connector-object-store/src/lib.rs` | `stillflow-connector-object-store` | `#[async_trait] impl SourceConnector for ObjectStoreConnector` | No | Implements discovery, inspection, preview, and read for object-store tabular files. It does not expose `upload` through the `SourceConnector` trait. | Read-only through the connector trait; `upload` is only on `ObjectStorageAccess`. | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| `staged::stage_complete` / `stage_bytes` | `backend/crates/stillflow-connector-object-store/src/staged.rs` | `stillflow-connector-object-store` | `pub(crate) async fn stage_complete(...)`; `pub(crate) async fn stage_bytes(...)` | No; temporary local staging for reads | Writes remote text/JSON ranges or full streams into a `tempfile::TempDir` so the local tabular connector can read them. The temp directory is dropped when the stream ends. | Runtime-only temporary staging; not a final output location. | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| Test fixture writers (`ArrowWriter`, `fs::write`, `BufWriter`) | `backend/crates/stillflow-connector-local-tabular/tests/local_tabular.rs`, `backend/crates/stillflow-connector-object-store/tests/object_store_connector.rs`, `backend/crates/stillflow-connector-workbook/tests/workbook_connector.rs` | test code | Test-only file creation | No | Create CSV/TSV/JSON/NDJSON/Parquet/workbook fixtures for connector tests, or upload objects in object-store tests. | Not product code; no export runtime. | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| Arrow C Data Interface export in engine FFI | `backend/crates/stillflow-engine/src/ffi.rs` | `stillflow-engine` | `fn export_arrow_array(...)` and related helpers | No; internal execution interchange | Exports Arrow arrays through the Arrow C Data Interface during Polars/Arrow transition. It is not a file, byte-stream, or download export. | Runtime-only memory transfer. | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| `PlanNodeKind::Materialize { output_label }` | `backend/crates/stillflow-plan/src/plan.rs` | `stillflow-plan` | `Materialize { output_label: String }` | No | A logical plan output label used during materialization; it is not an export filename or artifact name. | Logical plan serialization only. | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |

There is no backend symbol named `ExportArtifact`, `ExportRequest`, `ExportFormat`,
`ExportPolicy`, `ExportInputRef`, `OutputLocation`, export manifest, export digest,
download API, or export job/run in the inventory base.

## 2. Snapshot and Artifact read facts

### 2.1 `SnapshotStore::load_manifest`

- Path: `backend/crates/stillflow-storage/src/store.rs`
- Signature: `pub fn load_manifest(&self, snapshot_id: Uuid) -> Result<SnapshotManifest, StorageError>`
- Behavior:
  - opens `metadata.sqlite3`, selects only `state = visible` snapshots;
  - revalidates `DatasetSnapshot` version, identities, `LogicalSchema`, `LogicalSchemaFingerprint`, `SnapshotStats`, and per-partition rows;
  - returns `SnapshotManifest` with ordered partitions.
- This is the read path a future exporter could use to obtain committed Snapshot metadata without proposing a new interface.

### 2.2 `SnapshotStore::read_batches` and `SnapshotBatchReader`

- Path: `backend/crates/stillflow-storage/src/store.rs`
- Signature: `pub fn read_batches(&self, snapshot_id: Uuid) -> Result<SnapshotBatchReader, StorageError>`; `impl Iterator for SnapshotBatchReader` with `Item = Result<BatchEnvelope, StorageError>`.
- Behavior:
  - acquires a reader `ActivityGuard` for the reader lifetime;
  - loads a visible manifest;
  - yields one `BatchEnvelope` per manifest partition in ascending partition sequence;
  - each `read_partition` verifies:
    - final snapshot directory is not a symlink and is a directory;
    - partition file is not a symlink and is a regular file;
    - file length equals `SnapshotPartition.stored_byte_count`;
    - SHA-256 digest equals `SnapshotPartition.digest`;
    - Parquet decodes with the canonical Arrow schema derived from the logical schema;
    - decoded row count equals `SnapshotPartition.row_count`;
    - exactly one `RecordBatch` is produced per partition.
- This is a committed-Snapshot read capability, not a generic Artifact read handle.

### 2.3 Immutable Parquet partition layout

- Path: `backend/crates/stillflow-storage/src/store.rs`
- Layout:
  - `<root>/.stillflow.lock`;
  - `<root>/metadata.sqlite3`;
  - `<root>/staging/<snapshot_id>/<sequence:010>.parquet`;
  - `<root>/partitions/<snapshot_id>/<sequence:010>-<sha256>.parquet`.
- One non-empty input envelope becomes one final Parquet partition. Zero-row envelopes consume input sequence numbers but do not create physical partitions.
- `SnapshotManifest::try_new` requires contiguous zero-based partition sequences and exact row/byte totals against `DatasetSnapshot.stats`.

### 2.4 Digest verification

- Path: `backend/crates/stillflow-storage/src/digest.rs` and `store.rs`
- `ContentDigest` is SHA-256 over complete finalized Parquet file bytes.
- `load_manifest` does not read partition bytes; `read_batches` and `verify_snapshot` do.
- Digest mismatch, missing file, symlink, length mismatch, schema mismatch, row-count mismatch, or extra batch fail closed as `StorageError::Integrity`.

### 2.5 Logical schema and row ordering

- Each `BatchEnvelope` carries a validated `LogicalSchema`, `LogicalSchemaFingerprint`, `source_asset_id`, and `sequence`.
- `SnapshotBatchReader` yields partitions in manifest/partition-sequence order.
- The storage layer preserves the append order as partition sequence; there is no global row-sort contract in the snapshot reader beyond that order and the row order written into each Parquet file.
- A future exporter can rely on partition order but must freeze any stronger row-ordering guarantee in X-C0.

### 2.6 Reader concurrency and maintenance gates

- Path: `backend/crates/stillflow-storage/src/store.rs` and `manifest.rs`
- Defaults: `MAX_ACTIVE_READERS = 64`, `MAX_ACTIVE_PUBLISHERS = 8`.
- `acquire_activity` fails fast with `StorageError::Busy` when limits are exceeded.
- `SnapshotBatchReader` holds a reader guard for its whole lifetime.
- `recover` and `collect_garbage` acquire a maintenance gate that excludes readers and publishers.
- `SnapshotStore::open` holds an exclusive OS file lock on `<root>/.stillflow.lock` for the store lifetime.

### 2.7 Tombstone and retention behavior

- `tombstone_snapshot(snapshot_id, tombstoned_at)` changes a visible snapshot to `tombstoned` in one SQLite transaction; ordinary `load_manifest`/`read_batches` only see visible snapshots.
- `collect_garbage(now, retention, max_candidates)` removes tombstoned snapshot final directories and deletes their SQLite rows only after the retention cutoff.
- `recover(now, stale_after, max_candidates)` cleans stale `publications` journal rows, unpublished staging/final residue, and orphan staging directories.
- There is no retention policy for Export Artifacts because no Export Artifact exists.

## 3. Existing format capabilities

The matrix uses these statuses:

- `ingest/read`: implemented reader/parser path on the base;
- `internal encode`: an internal representation or internal serialization is written/encoded on the base;
- `test-only write`: fixture writer in test code only;
- `final export`: user-facing product export runtime;
- `frontend-only`: frontend label, sample, or browser-local simulation only;
- `missing`: no implementation evidence on the base.

| Format | Ingest/read | Internal encode | Test-only write | Final export | Frontend-only | Current type/null/timestamp evidence |
| --- | --- | --- | --- | --- | --- | --- |
| CSV | Implemented | No final CSV encode; used only as source decode | Yes (`fs::write` in tests) | Missing | Yes (`Export CSV` node label, `customer_report.csv` sample, embedded sample CSV in DuckDB) | `read.rs` uses configured delimiter/quote/header; empty delimited field is null only when field is nullable or `LogicalType::Null`; floats must be finite; dates must match `%Y-%m-%d`; timestamps must match RFC3339 for timezone-aware or `%Y-%m-%dT%H:%M:%S%.f` / `%Y-%m-%d %H:%M:%S%.f` for naive; CSV/TSV/JSON/NDJSON/Parquet reader source is `backend/crates/stillflow-connector-local-tabular/src/read.rs` |
| TSV | Implemented | No final TSV encode | Yes (`fs::write` in tests) | Missing | Not present as a frontend export choice | Same delimited reader path with fixed tab separator; `tsv.has_header` from config; no TSV writer exists |
| JSON | Implemented | Internal projected-row JSON encoding only while decoding | Yes (`fs::write` in tests) | Missing | Not present as a frontend export choice | `read.rs` parses one top-level array through `JsonObjectStream`; object fields must match established schema; nested List/Struct are validated recursively; binary values are rejected; floats must be finite; no JSON writer/export path |
| JSONL | Implemented via `.jsonl` extension mapped to NDJSON | Internal projected-row JSON-lines encoding only while decoding | Yes (`fs::write` in tests) | Missing | Not present as a frontend export choice | Same reader path as NDJSON; one object per non-empty line; `JsonFormat::JsonLines` is used inside Polars decoding; no JSONL writer/export path |
| NDJSON | Implemented via `.ndjson` and `.jsonl` | Same internal JSON-lines decode path | Yes (`fs::write` in tests) | Missing | Not present as a frontend export choice | Same as JSONL; object-per-line validation in `read.rs`; no NDJSON writer/export path |
| Parquet | Implemented | Implemented as internal Snapshot payload (`ArrowWriter` in `stillflow-storage`) | Yes (`ArrowWriter` in connector tests) | Missing as product export; Snapshot Parquet is internal immutable storage, not a user-facing download/export artifact | Yes (`Parquet` dataset labels, `web_events.parquet` sample) | Reader uses Polars/Parquet and Arrow schema mapping; storage writer uses Snappy compression and `MAX_BATCH_ROWS` row-group target; no export Parquet writer/policy exists |
| Arrow IPC | Missing as a product surface | Not used in source; `arrow-ipc` appears only in `Cargo.lock` as a transitive dependency | Not used in tests as product writer | Missing | Not present | No source code uses Arrow IPC for export/read on the base |
| Instruction JSONL | Missing | Missing | Missing | Missing | Not present | No schema, type, writer, reader, or frontend option exists |
| Chat JSONL | Missing | Missing | Missing | Missing | Not present | No schema, type, writer, reader, or frontend option exists |

## 4. Filesystem and object-store write facts

| Area | Current facts | Exact path | Base SHA |
| --- | --- | --- | --- |
| Local tabular read roots | `allowedRoots` must be absolute; `RootSet::open` opens each root with `cap-std` no-follow directory handles; traversal, symlinked roots/files, and unsafe locator components are rejected; connector opens files read-only. No write path exists in this connector. | `backend/crates/stillflow-connector-local-tabular/src/config.rs`, `backend/crates/stillflow-connector-local-tabular/src/path.rs` | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| Snapshot managed root | `SnapshotStore::open` requires a non-symlink directory root, creates `staging`/`partitions`, holds an exclusive `.stillflow.lock`, and opens `metadata.sqlite3`; staged partition files use `create_new(true)`; final directory creation is exact; install uses `fs::rename`; Unix directory `sync_all` is used for final/partition roots, non-Unix is a no-op. | `backend/crates/stillflow-storage/src/store.rs` | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| Snapshot cleanup/recovery | `SnapshotWriter::drop` best-effort removes staging/installed final dirs and deletes the publication row; `recover` removes stale publication residue and orphan staging; `collect_garbage` removes tombstoned snapshots after retention. No export temp/output cleanup exists. | `backend/crates/stillflow-storage/src/store.rs` | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| Object-store local provider | Local object storage root must be absolute and must not traverse symlinks; `StoreAccess::validate_internal_local_path` rejects symlinked components; reads and uploads are routed through `object_store::LocalFileSystem` with a configured prefix. | `backend/crates/stillflow-connector-object-store/src/access.rs`, `backend/crates/stillflow-connector-object-store/src/config.rs` | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| Object-store S3 provider | S3-compatible builder supports bucket/region/endpoint/path-style/anonymous/HTTP-dev flags; credentials are resolved through `ObjectStoreCredentialResolver` and consumed as ephemeral `S3CredentialMaterial`; secrets are redacted in `Debug`. | `backend/crates/stillflow-connector-object-store/src/access.rs`, `backend/crates/stillflow-connector-object-store/src/credentials.rs`, `backend/crates/stillflow-connector-object-store/src/config.rs` | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| Object-store upload semantics | `ObjectStorageAccess::upload` streams through `put_multipart`, buffers 5 MiB parts, enforces `MAX_UPLOAD_CHUNKS = 1_000_000` and `max_object_bytes` (default 1 TiB), aborts on failure/cancellation, and writes empty objects via `put`. No create-new guard or collision policy is present in the code. This is a connector byte-layer capability, not an export API. | `backend/crates/stillflow-connector-object-store/src/access.rs`, `backend/crates/stillflow-connector-object-store/src/config.rs` | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| Object-store temporary staging for reads | Text/JSON/NDJSON remote objects are staged into `tempfile::TempDir` files for local-tabular reads; complete remote streams are written to temporary files; the temp directory is tied to the read stream lifetime. This is not final output publication. | `backend/crates/stillflow-connector-object-store/src/staged.rs` | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| Credential references and error redaction | `SourceConnection` stores only `CredentialRef` and rejects secret field names in config; `ObjectStoreCredentialResolver` returns ephemeral material with redacted `Debug`; object-store errors are mapped to sanitized categories/details without secret values. | `backend/crates/stillflow-core/src/domain/connection.rs`, `backend/crates/stillflow-connector-object-store/src/credentials.rs`, `backend/crates/stillflow-connector-object-store/src/access.rs` | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |

Object-store read support (list/head/range/stream) is not evidence of object-store export support. The only write capability on the base is the connector `ObjectStorageAccess::upload` byte layer, which is not wired into a product export path.

## 5. Existing identity, digest and provenance facts

| Fact | Current definition/use | Exact path | Base SHA |
| --- | --- | --- | --- |
| `ContentDigest` | SHA-256 over complete finalized Snapshot Parquet file bytes; stored as `partitions.sha256`; serialized as lowercase 64-character hex. It is a partition content digest, not an export digest. | `backend/crates/stillflow-storage/src/digest.rs`, `backend/crates/stillflow-storage/src/store.rs` | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| `LogicalSchemaFingerprint` | `stillflow-schema-fnv1a64x4-v1`; non-security 256-bit index over serialized logical schema JSON; stored in Arrow schema metadata and SQLite `snapshots.schema_fingerprint`; used with complete schema equality in storage append checks. It is an index fingerprint, not a content digest. | `backend/crates/stillflow-core/src/batch.rs`, `backend/crates/stillflow-storage/src/store.rs` | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| `DatasetSnapshot` identity | `version`, `id`, `dataset_id`, `session_id`, `source_asset_id`, `schema`, `schema_fingerprint`, `stats`, `lineage`, `quality_score`, `created_at`; IDs are caller-injected and nil is rejected. | `backend/crates/stillflow-core/src/domain/snapshot.rs` | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| `SnapshotPartition` identity | `sequence`, `row_count`, `stored_byte_count`, `digest`; sequence is zero-based and contiguous. | `backend/crates/stillflow-storage/src/manifest.rs` | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| Dataset/session/source references | `DatasetSnapshot` carries `dataset_id`, `session_id`, `source_asset_id`; `BatchEnvelope` carries `source_asset_id`; `SnapshotDraft` carries the same identities plus `lineage`. | `backend/crates/stillflow-core/src/domain/snapshot.rs`, `backend/crates/stillflow-core/src/batch.rs`, `backend/crates/stillflow-storage/src/manifest.rs` | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| Lineage | `BTreeSet<Uuid>` in `DatasetSnapshot`/`SnapshotDraft`; no nil lineage IDs; serialized as `lineage_json` in SQLite. | `backend/crates/stillflow-core/src/domain/snapshot.rs`, `backend/crates/stillflow-storage/src/store.rs` | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| Engine/build/contract versions | `ENGINE_CONTRACT_VERSION = 1`; `PLAN_VERSION = 1`; `BATCH_ENVELOPE_VERSION = 1`; `LOGICAL_SCHEMA_VERSION = 1`; `DATASET_SNAPSHOT_VERSION = 1`; `STORAGE_SCHEMA_VERSION = 1`. | `backend/crates/stillflow-engine/src/lib.rs`, `backend/crates/stillflow-plan/src/plan.rs`, `backend/crates/stillflow-core/src/batch.rs`, `backend/crates/stillflow-core/src/logical.rs`, `backend/crates/stillflow-core/src/domain/snapshot.rs`, `backend/crates/stillflow-storage/src/manifest.rs` | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| Artifact / ExportArtifact identifiers | None exist on the base. No `Artifact`, `ArtifactRef`, `ExportArtifact`, export manifest, export digest, or Provenance Header type/field is present. | Search over `backend/crates` and `src` | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |

Content digest, identity, index fingerprint, and provenance are distinct facts in the current code. No canonical export digest is invented in this document.

## 6. Job, Run and Event integration facts

| Capability | Current fact | Exact path | Base SHA |
| --- | --- | --- | --- |
| Export Job | Missing. No `Job` or `ExportJob` type/repository/table/state machine exists. | `backend/crates` (no symbol) | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| Export Run | Missing. `ExecutionEngine::materialize` is a direct async call with a run-gate semaphore, not a persisted `Run` or `ExportRun`. | `backend/crates/stillflow-engine/src/engine.rs`, `backend/crates/stillflow-engine/src/lib.rs` | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| Export progress | Missing. No export progress type or event exists. Frontend progress is browser-local run progress. | `src/App.tsx`, `src/components/Header.tsx`, `src/utils/duckdb.ts` | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| Export cancellation | Missing at export level. Call-level cancellation/deadline exists through `RequestContext` and object-store `run_control`, but there is no Export Job cancel state or endpoint. | `backend/crates/stillflow-core/src/request/mod.rs`, `backend/crates/stillflow-connector-object-store/src/access.rs` | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| Export events | Missing. `IngestionEvent` exists with `RelationshipKind::Produces` declared but no export mapper/event usage; no event repository exists. | `backend/crates/stillflow-core/src/events/mod.rs` | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| Export recovery | Missing. `SnapshotStore::recover` handles Snapshot publication recovery only; no export staging/job recovery exists. | `backend/crates/stillflow-storage/src/store.rs` | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| Download / Artifact-read operation | Missing. `SnapshotStore::read_batches` is a storage-level Snapshot read, not a generic Artifact/download API; `stillflow-api` has no HTTP routes. | `backend/crates/stillflow-storage/src/store.rs`, `backend/crates/stillflow-api/src/lib.rs` | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |

Anything in the X phase that depends on a Job/Run/Event/Artifact runtime is `blocked by E5`; anything that depends on a future Verification/RejectedRows/Quality artifact contract is `blocked by E4` or later X-C0, not implemented on this base.

## 7. Frontend and product-surface inventory

| Frontend item | Classification | Current behavior | Exact path | Base SHA |
| --- | --- | --- | --- | --- |
| Header status label `Published` | presentation-only | Static label; no export/download action. | `src/components/Header.tsx` | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| `Export CSV` palette item (`t7`) | mock / presentation-only | Clicking adds an `export` node to the canvas; no file output or backend call occurs. | `src/data.ts`, `src/components/ObjectPalette.tsx`, `src/components/PipelineCanvas.tsx` | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| Export node (`n5`) | browser-local mock | The node appears as `Export CSV` / `Write cleaned data`; running it executes `CREATE OR REPLACE TABLE ... AS SELECT * FROM prevTable` in in-browser DuckDB. It does not write a file, invoke a backend, or produce a downloadable artifact. | `src/data.ts`, `src/utils/duckdb.ts`, `src/components/DetailPanel.tsx`, `src/components/PipelineCanvas.tsx` | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| Output dataset labels (`customer_report.csv`, `clean_customers`) | presentation-only | Static sample list in the Dataset panel; no read/write/download behavior. | `src/data.ts`, `src/components/DatasetPanel.tsx` | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| `Preview Result` button | mock | Shows a toast `Result preview opened`; no actual preview panel or data download. | `src/components/DetailPanel.tsx` | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| `Run All` / `Run From Here` | browser-local | Executes sample data through DuckDB-WASM in the browser; no network/backend calls exist in `src` (no `fetch`, `axios`, `XMLHttpRequest`, `/api`, WebSocket, or EventSource). | `src/App.tsx`, `src/utils/duckdb.ts` | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| Sample embedded CSV | browser-local | `CUSTOMERS_CSV` is loaded into DuckDB-WASM; it is sample data, not an export fixture. | `src/utils/sample-customers.ts`, `src/utils/duckdb.ts` | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| Selectable export formats / filename / location controls | missing | No format selector, filename field, destination control, progress/completion state, or download helper exists in frontend source. | `src` | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |
| Instruction/Chat JSONL options | missing | No frontend option or schema exists. | `src` | `89aab2551b8f73a32ed575bf75b3e3866b39d37c` |

Frontend labels are not treated as a frozen backend contract.

## 8. Missing-object matrix

Statuses: `implemented`, `placeholder`, `missing`, `blocked by E4`, `blocked by E5`.

| Object | Status | Current evidence / dependency |
| --- | --- | --- |
| `ExportRequest` | `missing` | No export request type exists; X-C0 has not frozen its fields. |
| `ExportFormat` | `missing` | No typed export-format enum exists; only reader formats and frontend labels exist. |
| `ExportPolicy` | `missing` | No policy object exists; X-C0 must decide semantics. |
| `ExportInputRef` | `missing` | No export input reference type exists. |
| `OutputLocation` | `missing` | No output-location type exists; object-store upload is a connector byte layer, not an export destination contract. |
| `ExportArtifact` | `missing` | No artifact type exists; E5 inventory already records generic `Artifact`/`ArtifactRef` as missing. |
| export manifest | `missing` | No export manifest type/file exists; `SnapshotManifest` is Snapshot-specific. |
| export content digest | `missing` | `ContentDigest` is a Snapshot partition digest; no export content digest exists. |
| Export Job / Run | `blocked by E5` | No generic Job/Run exists; E5 runtime is not merged. |
| export progress Event | `blocked by E5` | No export event or generic event repository exists; `IngestionEvent` is the only event type. |
| cancellation and deadline | `blocked by E5` | Call-level `RequestContext` cancellation/deadline exists, but no export job cancellation/deadline state machine exists. |
| restart recovery | `blocked by E5` | Snapshot publication recovery exists; no export job/temp-output recovery exists. |
| Artifact download/read handle | `blocked by E5` | `SnapshotBatchReader` is Snapshot-specific; no generic Artifact read/download handle exists. |
| retention and deletion | `blocked by E5` | Snapshot tombstone/GC exists; no Export Artifact retention/deletion policy exists. |
| Instruction JSONL schema | `missing` | No schema exists; roadmap says it may be approved only with a separate typed schema. |
| Chat JSONL schema | `missing` | No schema exists; roadmap says it may be approved only with a separate typed schema. |

## 9. Ownership and dependency facts

Accepted dependency direction from `AGENTS.md`:

```text
stillflow-api -> stillflow-engine
stillflow-engine -> stillflow-plan, stillflow-connectors, stillflow-storage
stillflow-plan -> stillflow-core
stillflow-connectors -> stillflow-core
stillflow-storage -> stillflow-core
stillflow-core -> no workspace crate
```

Current ownership relevant to output:

| Capability / type | Current owner crate | Exact path |
| --- | --- | --- |
| `BatchEnvelope` / `LogicalSchemaFingerprint` | `stillflow-core` | `backend/crates/stillflow-core/src/batch.rs` |
| `DatasetSnapshot` / `SnapshotStats` | `stillflow-core` | `backend/crates/stillflow-core/src/domain/snapshot.rs` |
| `SnapshotStore` / `SnapshotWriter` / `SnapshotBatchReader` / `SnapshotManifest` / `ContentDigest` | `stillflow-storage` | `backend/crates/stillflow-storage/src/store.rs`, `backend/crates/stillflow-storage/src/manifest.rs`, `backend/crates/stillflow-storage/src/digest.rs` |
| `ExecutionEngine` / `ExecutionIdentities` | `stillflow-engine` | `backend/crates/stillflow-engine/src/engine.rs`, `backend/crates/stillflow-engine/src/lib.rs` |
| Connector read/preview/stream | `stillflow-connectors` + adapter crates | `backend/crates/stillflow-connectors/src/connector.rs`, `backend/crates/stillflow-connector-local-tabular/src/lib.rs`, `backend/crates/stillflow-connector-object-store/src/lib.rs` |
| API surface | `stillflow-api` (placeholder) | `backend/crates/stillflow-api/src/lib.rs` |

Non-binding ownership candidates for future export objects (candidates only; not frozen):

| Missing capability | Candidate owner | Rationale preserving dependency direction |
| --- | --- | --- |
| `ExportRequest`, `ExportFormat`, `ExportPolicy`, `ExportInputRef`, `OutputLocation`, `ExportArtifact` domain values | `stillflow-core` | Stable public domain contracts belong in the lowest layer; API/engine/storage can depend on them. |
| Export manifest/digest persistence and retention | `stillflow-storage` | Control-plane SQLite and immutable payload persistence are already owned by `stillflow-storage`. |
| Export encoding/writer runtime | `stillflow-engine` (or an engine-owned adapter) | The engine owns execution, cancellation, and Snapshot publication; final export should follow the same execution boundary. |
| Export Job/Run/Event/API integration | `stillflow-engine` + `stillflow-api` | Job execution belongs in engine; HTTP operations belong in API; both preserve the dependency direction. |
| Object-store export destination transport | `stillflow-connector-object-store` / `stillflow-storage` | `ObjectStorageAccess::upload` already provides a byte-layer write seam; a future export destination would need a contract, not an inferred capability. |

No crate is added by this inventory.

## 10. X-C0 decision inputs

This table records decisions X-C0 must later freeze. It does not resolve them.

| Decision | Current fact / input | Decision dependency |
| --- | --- | --- |
| Eligible committed input objects | Only `DatasetSnapshot` + `SnapshotManifest` are committed immutable objects on the base; `SnapshotBatchReader` can read them. No generic Artifact read exists. | E5 Artifact contract; X-C0 input-object definition. |
| Whether rejected rows and reports may be exported | RejectedRows/Verification/Quality objects are not implemented on main; E4 proposals are `proposed / unmerged`. | `blocked by E4`; X-C0 policy. |
| Deterministic row and column ordering | `LogicalSchema` preserves field order; `BatchEnvelope` carries schema and sequence; `SnapshotBatchReader` yields partitions in manifest order. No global row-sort guarantee exists. | X-C0 ordering contract. |
| CSV/TSV delimiter, quoting, newline, encoding and BOM policy | Reader config supports configurable CSV delimiter/quote/header and fixed TSV tab; no writer policy exists. | X-C0 export format contract. |
| JSON versus JSONL record shape | Reader supports JSON array and NDJSON/JSONL object-per-line; no writer/export shape exists. | X-C0 export format contract. |
| Null, NaN/infinity, binary, date and timestamp/timezone encoding | Reader rejects non-finite floats, rejects binary in JSON, and validates dates/timestamps with strict formats; no export encoding contract exists. | X-C0 export format contract. |
| Nested List/Struct policy | `LogicalType` supports List/Struct; readers validate nested JSON; no export encoding for nested values exists. | X-C0 export format contract. |
| Parquet compression, row-group, schema and metadata policy | Snapshot writer uses Snappy, `MAX_BATCH_ROWS`, canonical Arrow schema with reserved metadata; this is internal Snapshot Parquet, not export Parquet policy. | X-C0 export format contract. |
| Single file versus partitioned output | Snapshot storage is multi-partition Parquet; no export partitioning policy exists. | X-C0 export format contract. |
| Deterministic-byte guarantees | No export writer exists; `ContentDigest` guarantees only Snapshot partition bytes; plan/schema fingerprints are non-security indexes. | X-C0 deterministic output contract. |
| Row, byte, partition, memory, time and temporary-storage limits | Existing bounds: `MAX_BATCH_ROWS`, `MAX_BATCH_BYTES`, Snapshot limits, engine deadlines, object upload limits; no export-specific limits exist. | X-C0 resource-bound contract. |
| Output filename and safe-root rules | Local tabular read roots use absolute allowed roots and no-follow traversal; Snapshot storage uses UUID-managed roots; no export filename/root contract exists. | X-C0 filesystem safety contract. |
| Create-new, overwrite and collision policy | Snapshot storage uses `create_new`/exact directories and rejects duplicate snapshot IDs; object-store `upload` has no create-new guard. No export collision policy exists. | X-C0 publication contract. |
| Staging, atomic publication and crash recovery | Snapshot publication has staged Parquet + rename + SQLite commit + recovery; no export staging/atomic publication exists. | X-C0 + E5 recovery contract. |
| Cancellation and deadline | `RequestContext` propagates cancellation/deadline through reads/engine/object-store; no export job cancellation exists. | `blocked by E5`; X-C0 deadline contract. |
| Content digest and Provenance Header | `ContentDigest` is Snapshot partition SHA-256; no export digest or Provenance Header exists. | X-C0 identity/provenance contract. |
| Retention and deletion | Snapshot tombstone/GC exists; no Export Artifact retention/deletion exists. | `blocked by E5`; X-C0 retention contract. |
| Local download versus object-store destination | No download API exists; object-store `upload` is an unexposed byte layer; local file export does not exist. | X-C0 + E5 API contract. |
| E5 Job/Run/Event and API integration | No Job/Run/Event/Artifact runtime or HTTP API exists on the base. | `blocked by E5`. |

## 11. Inventory closure

This document is bound to `main@89aab2551b8f73a32ed575bf75b3e3866b39d37c`. It records current implementation and test evidence only. It does not freeze X-C0, add an export runtime, add an ExportArtifact, add Axum endpoints, add download transport, add object-store export writes, or begin E4/E5 runtime work.
