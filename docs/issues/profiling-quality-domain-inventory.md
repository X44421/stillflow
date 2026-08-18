# Q0-D0 Profiling and Quality Domain Inventory

> Status: discovery-only, non-binding; inventory complete for the stated base.
>
> Issue: #65
>
> Inventory base: `main@89aab2551b8f73a32ed575bf75b3e3866b39d37c`
>
> Delivery: docs-only
>
> Q-C0 contract status: not frozen

## 1. Scope and methodology

This document records repository facts observed at:

`main@89aab2551b8f73a32ed575bf75b3e3866b39d37c`

It is an input to a future Q-C0 contract. It does not freeze profiling or
quality public fields, metric semantics, Quality score formulas, serialization
formats, persistence tables, artifact protocols, API endpoints, crate
dependencies, or runtime implementation.

PR #53 and PR #57 are read-only references for this inventory. They are not
merged, rebased, or cherry-picked into this branch. Behavior proposed by those
PRs is not treated as implemented on the inventory base.

Repository findings use the following implementation-status vocabulary:

- `implemented`: a concrete definition and/or executable behavior exists on the
  inventory base.
- `placeholder`: a name, type, enum value, or architectural slot exists, but the
  intended capability is not implemented.
- `missing`: no corresponding Q0 profiling/quality capability is implemented on
  the inventory base.
- `blocked by E5/Q-C0`: the decision or implementation depends on contracts
  that are not part of the inventory base.

Persistence is classified separately:

- `persisted`: concrete persistence write/read behavior exists.
- `runtime-only`: the value exists only during execution.
- `defined-but-not-persisted`: a domain definition exists, but no persistence
  path has been established by repository evidence.
- `unknown`: the inspected evidence is insufficient to classify persistence.

Serialization support alone is not considered evidence of persistence.

## 2. Current profiling/statistics/quality/finding capabilities

### 2.1 Inspection findings

- **Definition:** `backend/crates/stillflow-core/src/domain/metadata.rs`
- **Status:** `implemented`
- **Types:**
  - `InspectionFinding { code: String, message: String, severity: FindingSeverity }`
  - `FindingSeverity { Info, Warning, Error }`
  - `AssetMetadata.findings: Vec<InspectionFinding>`
- **Producers on the inventory base:**
  - Local tabular: `inspect.schema_inference_truncated`
    (`backend/crates/stillflow-connector-local-tabular/src/inspect.rs`).
  - Object-store text staging: `inspect.remote_source_range_truncated`
    (`backend/crates/stillflow-connector-object-store/src/staged.rs`).
  - Workbook: `workbook.explicit_selection_required`,
    `workbook.ambiguous_regions`, `workbook.no_data_region`,
    `workbook.analysis_truncated`, `workbook.formula_cached_values`,
    `workbook.merged_cells`, `workbook.merge_metadata_unavailable`,
    `workbook.hidden_sheet`, `workbook.hidden_metadata_unavailable`,
    `workbook.no_header_selected`
    (`backend/crates/stillflow-connector-workbook/src/inspect.rs`).
- **Persistence:** `runtime-only`. Findings are returned in `AssetMetadata` and
  are not written by `stillflow-storage`.
- **Notes:** These are schema/format/structure inspection findings. They are
  not yet Q0 deterministic data-quality findings with evidence, object/rule/node
  provenance, or canonical digests.

### 2.2 Workbook structural analysis

- **Definition:** `backend/crates/stillflow-core/src/domain/workbook.rs`
- **Status:** `implemented`
- **Fields of `WorkbookInspection`:**
  - `sheet_visibility`
  - `formula_cells: u64`
  - `merged_regions: Vec<CellRange>`
  - `hidden_rows: Vec<u32>`
  - `hidden_columns: Vec<u32>`
  - `region_candidates: Vec<WorkbookRegionCandidate>`
  - `analysis_truncated: bool`
- **`WorkbookRegionCandidate` fields:**
  - `range: CellRange`
  - `confidence: CandidateConfidence`
  - `non_empty_cells: u64`
  - `header_candidates: Vec<WorkbookHeaderCandidate>`
- **`WorkbookHeaderCandidate` fields:**
  - `row: u32`
  - `confidence: CandidateConfidence`
  - `score: u8`
- **Analyzer:** `backend/crates/stillflow-connector-workbook/src/analysis.rs`
  with configurable `analysis_rows`, `analysis_columns`,
  `max_region_candidates`, and `max_sheet_cells`
  (`backend/crates/stillflow-connector-workbook/src/config.rs`).
- **Persistence:** `runtime-only`; `WorkbookInspection` is embedded in
  `AssetMetadata.workbook` but no storage table writes it.
- **Notes:** This is structural workbook statistics (region/header confidence,
  populated-cell counts, formula presence), not column-level data profiling.

### 2.3 Sampling strategy

- **Definition:** `backend/crates/stillflow-core/src/domain/preview.rs`
- **Status:** `placeholder`
- **Enum:** `SamplingStrategy { Head, Reservoir, Random }`
- **Current behavior:** `PreviewRequest.sampling` defaults to `Head`; all
  implemented connectors reject `Reservoir` and `Random` with
  `ConnectorError::for_unsupported_capability("preview_sampling")`:
  - `backend/crates/stillflow-connector-local-tabular/src/lib.rs`
  - `backend/crates/stillflow-connector-object-store/src/lib.rs`
  - `backend/crates/stillflow-connector-workbook/src/lib.rs`
- **Notes:** Only bounded head/prefix sampling exists today. Reservoir/random
  are declared but not implemented.

### 2.4 Preview metadata

- **Definition:** `backend/crates/stillflow-core/src/domain/preview.rs`
- **Status:** `implemented`
- **Fields of `PreviewData`:**
  - `schema`
  - `batches`
  - `rows_returned`
  - `rows_truncated`
  - `bytes_returned`
  - `bytes_truncated`
  - `warnings: Vec<String>`
- **Persistence:** `runtime-only`. Preview payload is validated in memory and
  not persisted by storage on the inventory base.
- **Notes:** Preview returns truncation counters and warnings, but no per-column
  statistics or profile artifact.

### 2.5 Snapshot quality metadata

- **Definition:** `backend/crates/stillflow-core/src/domain/snapshot.rs`
- **Status:** `implemented` as an opaque persisted metadata field; **not**
  implemented as a computed Quality score.
- **Field:** `DatasetSnapshot.quality_score: Option<u8>`
- **Persistence:** `persisted` in SQLite `snapshots.quality_score`.
- **Notes:** Detailed facts are in Section 3.

### 2.6 `Profiled` event relationship

- **Definition:** `backend/crates/stillflow-core/src/events/mod.rs`
- **Status:** `placeholder`
- **Value:** `RelationshipKind::Profiled` exists in the event enum.
- **Notes:** No `Profile` object, `ProfileRequest`, `ProfileResult`, or event
  producer uses this relationship on the inventory base.

### 2.7 High-level Q0 objects

- `ProfileRequest`: `missing`.
- `ProfileResult` / `ColumnProfile` / `ColumnStatistics`: `missing`.
- `ProfileArtifact`: `missing`.
- `QualityReport`: `missing`.
- `QualityScore` formula/version: `missing`.
- Typed Q0 `Finding` with evidence/provenance: `missing`; only
  `InspectionFinding` exists.
- Profile/Quality storage tables or artifact writers: `missing`.

## 3. `DatasetSnapshot.quality_score` facts

### 3.1 Core definition and validation

- **Source:** `backend/crates/stillflow-core/src/domain/snapshot.rs`
- `DatasetSnapshot` stores `quality_score: Option<u8>` as a private field.
- Serialization uses a private `DatasetSnapshotData` with `qualityScore`.
- `try_new` / `try_from_parts` reject `quality_score` outside `0..=100`
  (`SnapshotError::InvalidQualityScore`).
- `quality_score()` is a public read-only accessor.
- `SnapshotStats` contains row/byte/partition totals only; it does not contain
  profiling or quality evidence.

### 3.2 Engine injection path

- **Source:** `backend/crates/stillflow-engine/src/lib.rs` and
  `backend/crates/stillflow-engine/src/engine.rs`
- `ExecutionIdentities.quality_score: Option<u8>` is caller-injected.
- `ExecutionRequest.identities` passes it to `SnapshotDraft::try_new`.
- `validate_identities` only checks `quality_score <= 100`; it does not compute,
  derive, or validate evidence for the score.
- The engine does not default `quality_score` when the caller passes `None`.
- Contract references: `docs/issues/issue-046-deterministic-engine-execution-contract.md`
  documents that `quality_score` comes from `identities.quality_score` and must
  not be generated by the engine.

### 3.3 Storage persistence path

- **Source:** `backend/crates/stillflow-storage/src/manifest.rs` and
  `backend/crates/stillflow-storage/src/store.rs`
- `SnapshotDraft.quality_score: Option<u8>` is carried into `DatasetSnapshot`
  by `build_snapshot`.
- SQLite migration creates:
  ```sql
  quality_score INTEGER CHECK (quality_score BETWEEN 0 AND 100)
  ```
  in the `snapshots` table.
- `commit_manifest` writes `quality_score` as `Option<i64>`.
- `load_manifest_inner` reads it back, revalidates `u8` conversion, and passes
  it to `DatasetSnapshot::try_from_parts`.
- The Parquet partitions contain tabular payload only; `quality_score` is
  control-plane metadata, not a column in the Arrow payload.

### 3.4 Computation fact

- **No backend computation exists.**
- The engine and storage validate the range but do not calculate the score from
  row, column, null, duplicate, distribution, or finding evidence.
- The only ad hoc Quality score computation in the repository is in the
  frontend DuckDB utility (Section 7), which is not backend-supported.
- Contract references: `docs/issues/issue-028-storage-implementation-contract.md`
  and `docs/development/backend-completion-execution-checklist.md` (Q-C0) treat
  the score as a Q0 decision, not as an implemented formula.

### 3.5 Test evidence

- Core: `rejects_invalid_versions_identities_quality_and_stats`
  (`backend/crates/stillflow-core/src/domain/snapshot.rs`).
- Engine: materialize round-trip asserts
  `manifest.snapshot().quality_score() == Some(95)`
  (`backend/crates/stillflow-engine/src/tests.rs`).
- Storage: `snapshot_is_invisible_until_commit_and_roundtrips_exactly` asserts
  `manifest.snapshot().quality_score() == Some(97)`
  (`backend/crates/stillflow-storage/src/store.rs`).

## 4. Connector sampling and statistics inventory

| Connector | Sampling implemented | Statistics/findings | Preview/read bounds |
| --- | --- | --- | --- |
| Local tabular (`stillflow-connector-local-tabular`) | `Head` only; `Reservoir`/`Random` rejected | Schema inference on bounded rows/bytes; `row_count` for Parquet; `inspect.schema_inference_truncated` finding; no column statistics | `inference_rows` default 10,000 / `inference_bytes` default 8 MiB; preview row/byte limits; read `batch_size` 1..=65536 |
| Object store (`stillflow-connector-object-store`) | `Head` only; `Reservoir`/`Random` rejected | Text staging truncation finding; Parquet inspect returns `row_count` and empty findings; range reads bounded by `max_preview_source_bytes` default 64 MiB; no column statistics | `max_object_bytes`; `max_preview_source_bytes`; Parquet preview uses `PREVIEW_BATCH_ROWS`; read streams full object with bounded envelopes |
| Workbook (`stillflow-connector-workbook`) | `Head` only; `Reservoir`/`Random` rejected | Structural analysis: region candidates, header scores/confidence, non-empty cells, formula count, merged regions, hidden rows/columns, `analysis_truncated`; no column data statistics | `max_sheet_cells`, `analysis_rows` default 10,000, `analysis_columns` default 256, `max_region_candidates` default 128; preview row/byte limits; read batches bounded by row and byte estimates |

Exact source paths:

- Local tabular config: `backend/crates/stillflow-connector-local-tabular/src/config.rs`
- Local tabular inspect: `backend/crates/stillflow-connector-local-tabular/src/inspect.rs`
- Object-store config: `backend/crates/stillflow-connector-object-store/src/config.rs`
- Object-store staged text: `backend/crates/stillflow-connector-object-store/src/staged.rs`
- Object-store Parquet: `backend/crates/stillflow-connector-object-store/src/parquet.rs`
- Workbook config: `backend/crates/stillflow-connector-workbook/src/config.rs`
- Workbook analysis: `backend/crates/stillflow-connector-workbook/src/analysis.rs`
- Preview contracts: `backend/crates/stillflow-core/src/domain/preview.rs`

## 5. Engine reusable bounded scan/chunk/memory/cancellation

The inventory base has no engine-level node Preview runtime (that is PR #53,
read-only). What exists is the bounded materialize path and connector streaming
primitives, which are reusable for a future bounded profiler.

### 5.1 Request-level cancellation/deadline

- **Source:** `backend/crates/stillflow-core/src/request/mod.rs`
- `RequestContext` carries `CancellationToken` and `Option<Instant>` deadline.
- `ensure_active()` returns `Cancelled` or `Timeout` errors.
- Connector registry and engine call it at operation boundaries.

### 5.2 Bounded scan and stream

- **Source:** `backend/crates/stillflow-core/src/batch.rs`
- `BatchEnvelope` enforces `MAX_BATCH_ROWS = 65_536` and
  `MAX_BATCH_BYTES = 64 MiB`.
- `ReadRequest.batch_size` is constrained to `1..=65536`
  (`backend/crates/stillflow-core/src/domain/read.rs`).
- `ConnectorRegistry::read_batches` attaches request context and validates
  capabilities (`backend/crates/stillflow-connectors/src/registry.rs`).
- Connectors return `RawBatchStream` that the registry wraps into a
  context-aware `BatchStream`.

### 5.3 Engine chunking and memory bounds

- **Source:** `backend/crates/stillflow-engine/src/engine.rs`,
  `backend/crates/stillflow-engine/src/predict.rs`,
  `backend/crates/stillflow-engine/src/remainder.rs`,
  `backend/crates/stillflow-engine/src/memory.rs`
- `ExecutionEngine::materialize` consumes connector envelopes through
  `stream_and_publish`.
- `consume_envelope` splits each incoming envelope into feasible chunks with
  `largest_feasible_k`, using `predict` to keep the Polars working set under
  `MAX_BATCH_BYTES`.
- `CanonicalRebatcher` repacks transformed rows into canonical output envelopes,
  respecting `pack_limit` (request batch size) and `MAX_BATCH_BYTES`.
- `MemoryTracker` enforces `MAX_LIVE_COLUMNAR_PAYLOADS = 3` and
  `MAX_ENGINE_PEAK_BYTES`; `MemoryReport` records `chunk_count`,
  `min_chunk_rows`, and phase peaks.
- `MAX_ENGINE_CONCURRENT_RUNS = 4` gates concurrent materialize runs with a
  semaphore.

### 5.4 Cancellation points in materialize

- Before `begin_snapshot`.
- Before each connector stream item.
- Before each `largest_feasible_k` chunk.
- Before each storage `append`.
- There is no separate `ensure_active` immediately before
  `SnapshotWriter::commit` in `engine.rs` on this base. The commit path is
  reached only after the stream loop and `CanonicalRebatcher::finish` succeed;
  a cancellation that occurs after the last append and before `commit` is not
  separately checked by the engine code.

### 5.5 Reuse assessment

The chunking, memory accounting, deadline/cancellation propagation, and
bounded envelope machinery are implemented and can be reused by Q0. There is no
profiling-specific accumulator, sampling state, or artifact writer on the base.

## 6. Storage persistence facts

### 6.1 Existing tables

`stillflow-storage` creates schema version 1 with three tables
(`backend/crates/stillflow-storage/src/store.rs`):

- `publications(snapshot_id, started_at_utc)`
- `snapshots(id, version, dataset_id, session_id, source_asset_id,
  schema_json, schema_fingerprint, row_count, stored_byte_count,
  partition_count, lineage_json, quality_score, created_at_utc, state,
  tombstoned_at_utc)`
- `partitions(snapshot_id, sequence, row_count, stored_byte_count, sha256)`

### 6.2 Profile/Finding/QualityReport persistence

- **Profile:** `missing`. No `profiles` table, no `ProfileArtifact` writer, no
  profile manifest or partition type.
- **Finding:** `missing` as persisted Q0 findings. `AssetMetadata.findings` are
  `runtime-only`; no `findings` table exists.
- **QualityReport:** `missing`. No `quality_reports` table, no QualityReport
  artifact, no report digest or provenance row.
- **Quality score:** `persisted` only as the opaque `snapshots.quality_score`
  metadata column (Section 3).

### 6.3 Reusable storage pattern

`SnapshotStore` already provides an immutable, atomic, journaled publication
pattern for snapshot manifests and Parquet partitions
(`docs/issues/storage-publication-recovery-inventory.md`). It is not a generic
artifact store: no generic `ArtifactRef`, Profile artifact, or QualityReport
publication exists on the base.

## 7. Frontend Profile/Quality display facts

### 7.1 Types

- `src/types.ts` defines `PipelineMetrics.qualityScore`, `PreviewColumn`
  (`nullCount`, `distinctCount`), and `DataPreviewResult`.
- `PreviewColumn` / `DataPreviewResult` are currently unused by UI code on the
  inventory base.

### 7.2 Computation and display

- `src/utils/duckdb.ts` computes pipeline metrics in-browser with DuckDB-WASM:
  `rowsIn`, `rowsOut`, `duplicates`, `missing`, `nullColumns`, `qualityScore`,
  `duration`, and a deterministic `memory` estimate.
- The Quality score formula is ad hoc:
  `100 - nullCount / (totalRows * 4) * 100`, clamped to `0..=100`.
- `src/components/DetailPanel.tsx` displays `metrics.qualityScore` and labels it
  `Good` / `Fair` / `Poor`.
- `src/data.ts` provides mock datasets and initial pipeline nodes.
- `src/App.tsx` calls `initDuckDB`, `loadSampleData`, and `runFullPipeline`
  locally; it does not call a backend API.

### 7.3 Backend support

- `backend/crates/stillflow-api/src/lib.rs` is a placeholder with only
  `crate_name()`; there is no HTTP route or serialized Profile/Quality payload.
- No backend `Profile` or `QualityReport` type exists to serve the frontend.
- **Conclusion:** the frontend Quality display is mock/local DuckDB computation,
  not backend-supported profiling/quality.

## 8. Q-C0 missing object matrix

| Object / capability | Status on `main@89aab25` | Evidence / gap |
| --- | --- | --- |
| `ProfileRequest` | `missing` | No request type, bounds, supported logical types, or sampling contract exists |
| `ProfileResult` / `ColumnProfile` / `ColumnStatistics` | `missing` | No per-column metric types or accumulators exist |
| `ProfileArtifact` | `missing` | No artifact type, writer, digest, or storage table exists |
| `QualityReport` | `missing` | No report type, artifact, finding list, or provenance exists |
| Quality score formula/version | `missing` | `quality_score` is caller-injected opaque metadata; no formula, version, or missing-evidence policy |
| Typed Q0 `Finding` | `missing` | Only `InspectionFinding` exists; no evidence, category, or provenance fields |
| Q0 `FindingCategory` | `missing` | No schema/text/duplicate/privacy/distribution/leakage category enum |
| Profile/Quality persistence | `missing` | No `profiles`, `findings`, or `quality_reports` table in `stillflow-storage` |
| Profile/Quality Job/API | `missing` | No job, run, status, cancel, or artifact-read API for profiling/quality |
| Reservoir/Random sampling | `placeholder` | Enum exists; all connectors reject non-Head sampling |
| `InspectionFinding` | `implemented` (inspection only) | `AssetMetadata.findings` returned by inspect, not persisted |
| `WorkbookInspection` | `implemented` (structural only) | Region/header/formula analysis, not column data profiling |
| `quality_score` metadata field | `implemented` (opaque persisted) | `DatasetSnapshot` + SQLite `snapshots.quality_score` |
| `RequestContext` cancellation/deadline | `implemented` | Reusable by a future bounded profiler |
| Engine chunk/memory machinery | `implemented` | `largest_feasible_k`, `CanonicalRebatcher`, `MemoryTracker` |
| Snapshot publication pattern | `implemented` | Could inform artifact publication, but no generic artifact exists |

## 9. Q-C0 decisions pending freeze

The checklist currently marks Q-C0 as `blocked` until E5 Artifact/Run ownership
is stable (`docs/development/backend-completion-execution-checklist.md`).
The following are inputs from that checklist plus repository facts. None are
frozen by this inventory.

1. **Exact vs sampled metrics**
   - Decide whether Q0 metrics are exact on the bounded scan or sampled.
   - Decide deterministic sampling seed/source and whether `Reservoir`/`Random`
     become real capabilities.
2. **`ProfileRequest` bounds**
   - Row, byte, time, memory, concurrency, and supported logical types.
   - Relationship to existing `ReadRequest` / `BatchEnvelope` limits.
3. **Core metrics**
   - Row/column/null/unique/duplicate metrics and their exact definitions.
4. **Numeric policy**
   - min/max/mean/distribution policy and whether histograms are bounded.
5. **Cardinality/top-values policy**
   - Top values, cardinality bound, and the forbidden unbounded exact
     cardinality rule.
6. **Utf8/Binary policy**
   - Length metrics and invalid-value policy.
7. **Finding categories**
   - Schema, text, duplicate, privacy, distribution, and leakage categories.
   - Whether `InspectionFinding` is promoted into Q0 findings or kept separate.
8. **Quality score**
   - Formula, version, missing-evidence behavior, and whether the existing
     opaque `DatasetSnapshot.quality_score` becomes derived from a
     `QualityReport` or remains a separate caller-injected value.
9. **Artifact provenance/digest**
   - Profile/Quality artifact provenance, canonical digest, and ownership.
   - Whether `stillflow-storage` gains artifact tables/writers or a generic
     artifact layer is introduced first.
10. **Ownership boundaries**
    - Stable Q0 domain values in `stillflow-core`; profiler implementation in
      `stillflow-engine`; persistence in `stillflow-storage`; HTTP in
      `stillflow-api`.
11. **Forbidden semantics**
    - LLM-defined metrics, opaque scores without evidence, and unbounded exact
      cardinality remain forbidden by the checklist.

## 10. Crate ownership facts

| Type / capability | Current owner crate | Evidence |
| --- | --- | --- |
| `InspectionFinding`, `FindingSeverity`, `AssetMetadata` | `stillflow-core` | `backend/crates/stillflow-core/src/domain/metadata.rs` |
| `WorkbookInspection`, region/header candidates | `stillflow-core` | `backend/crates/stillflow-core/src/domain/workbook.rs` |
| `SamplingStrategy`, `PreviewRequest`, `PreviewData` | `stillflow-core` | `backend/crates/stillflow-core/src/domain/preview.rs` |
| `DatasetSnapshot.quality_score` | `stillflow-core` | `backend/crates/stillflow-core/src/domain/snapshot.rs` |
| `ExecutionIdentities.quality_score` | `stillflow-engine` | `backend/crates/stillflow-engine/src/lib.rs` |
| `MemoryTracker`, `CanonicalRebatcher`, chunk prediction | `stillflow-engine` | `backend/crates/stillflow-engine/src/memory.rs`, `remainder.rs`, `predict.rs` |
| SQLite snapshot metadata persistence | `stillflow-storage` | `backend/crates/stillflow-storage/src/store.rs` |
| HTTP/API surface | `stillflow-api` | `backend/crates/stillflow-api/src/lib.rs` (placeholder) |

Non-binding ownership candidates for Q0, preserving dependency direction:

- Stable Profile/Quality domain values: `stillflow-core`.
- Bounded streaming profiler and detectors: `stillflow-engine`.
- Profile/Quality persistence and artifact publication: `stillflow-storage`.
- Profile/Quality submit/status/cancel/read API: `stillflow-api`.

## 11. Inventory closure

This document is bound to `main@89aab2551b8f73a32ed575bf75b3e3866b39d37c`. It
records current implementation and test evidence only. It does not freeze Q-C0,
add a profiling API, add storage tables, or change PR #53 / PR #57.
