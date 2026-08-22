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
- `blocked by E4`: the decision or implementation depends on E4 contracts or
  runtime (PR #57) that are not part of the inventory base.
- `blocked by E5`: the decision or implementation depends on E5 Job/Run/Artifact
  or API ownership contracts that are not part of the inventory base.
- `blocked by Q-C0`: the decision or implementation depends on a future Q-C0
  contract and is not a repository fact on the inventory base.

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
- `ProfilePolicy`: `missing`.
- `DatasetProfile` / `ColumnProfile` / `ColumnStatistics`: `missing`.
- `ProfileReportArtifact`: `missing`.
- `QualityReport` / `QualityReportArtifact`: `missing`.
- `QualityScore` formula / `QualityScoreVersion`: `missing`.
- Typed Q0 `Finding`, `FindingCategory`, and `FindingEvidence`: `missing`; only
  `InspectionFinding` exists.
- Profile/Quality storage tables or artifact writers: `missing`.

### 2.8 Backend symbol and persistence table

The implementation base is `main@89aab2551b8f73a32ed575bf75b3e3866b39d37c`.
All paths are exact repository paths on that base. “Status” uses the
vocabulary in Section 1; “Persistence” uses the persistence vocabulary in
Section 1. This table is the required backend symbol and persistence
inventory.

| Backend symbol | Crate ownership / exact path | Current behavior / fields | Status | Persistence |
| --- | --- | --- | --- | --- |
| `InspectionFinding`, `FindingSeverity`, `AssetMetadata.findings` | `stillflow-core` / `backend/crates/stillflow-core/src/domain/metadata.rs` | `InspectionFinding { code: String, message: String, severity: FindingSeverity }`; produced by connector inspect paths (local tabular, object-store text staging, workbook) and returned in `AssetMetadata.findings` | `implemented` (inspection only) | `runtime-only` |
| `WorkbookInspection`, `WorkbookRegionCandidate`, `WorkbookHeaderCandidate` | `stillflow-core` / `backend/crates/stillflow-core/src/domain/workbook.rs`; analyzer in `backend/crates/stillflow-connector-workbook/src/analysis.rs` | Structural workbook statistics: sheet visibility, formula cells, merged regions, hidden rows/columns, region/header candidates and confidence; not column data profiling | `implemented` (structural only) | `runtime-only` |
| `SamplingStrategy` | `stillflow-core` / `backend/crates/stillflow-core/src/domain/preview.rs` | `enum SamplingStrategy { Head, Reservoir, Random }`; all connectors reject `Reservoir`/`Random` with `for_unsupported_capability("preview_sampling")` | `placeholder` | `runtime-only` |
| `PreviewData` | `stillflow-core` / `backend/crates/stillflow-core/src/domain/preview.rs` | `schema`, `batches`, `rows_returned`, `rows_truncated`, `bytes_returned`, `bytes_truncated`, `warnings: Vec<String>`; no per-column statistics | `implemented` | `runtime-only` |
| `SnapshotStats` | `stillflow-core` / `backend/crates/stillflow-core/src/domain/snapshot.rs` | Row/byte/partition totals; no profiling or quality evidence | `implemented` | `persisted` (as snapshot metadata columns) |
| `DatasetSnapshot.quality_score` | `stillflow-core` / `backend/crates/stillflow-core/src/domain/snapshot.rs` | `Option<u8>` private field; `try_new`/`try_from_parts` reject values outside `0..=100`; `quality_score()` read-only accessor | `implemented` (opaque persisted metadata; not computed) | `persisted` in SQLite `snapshots.quality_score` |
| `SnapshotDraft.quality_score` | `stillflow-storage` / `backend/crates/stillflow-storage/src/manifest.rs` | Carried into `DatasetSnapshot` by `build_snapshot`; not independently persisted | `implemented` | `runtime-only` (flows into persisted `snapshots.quality_score`) |
| `ExecutionIdentities.quality_score` | `stillflow-engine` / `backend/crates/stillflow-engine/src/lib.rs` | Caller-injected `Option<u8>`; engine passes it to `SnapshotDraft::try_new`; `validate_identities` only checks `<= 100` and does not compute evidence | `implemented` (caller-injected) | `runtime-only` |
| `RelationshipKind::Profiled` | `stillflow-core` / `backend/crates/stillflow-core/src/events/mod.rs` | Enum value exists; no `Profile` object, request, result, or producer uses it | `placeholder` | `runtime-only` |
| `Rule::Validate { predicate, severity, message }`, `ValidationSeverity` | `stillflow-plan` / `backend/crates/stillflow-plan/src/rule.rs` | Logical rule vocabulary is implemented and validated (`Warning`/`Error`, non-empty message, predicate expression shape); engine preflight, lowering, and prediction reject it as `EngineError::UnsupportedRule` | `implemented` as logical contract vocabulary; `blocked by E4` for engine runtime execution | `defined-but-not-persisted` (no E4 report artifact/table merged on the base) |
| `Rule::Deduplicate { keys }` | `stillflow-plan` / `backend/crates/stillflow-plan/src/rule.rs` | Logical rule vocabulary is implemented and validated (non-empty, de-duplicated keys); engine preflight, lowering, and prediction reject it as `EngineError::UnsupportedRule` | `implemented` as logical contract vocabulary; `blocked by E4` for engine runtime execution | `defined-but-not-persisted` (no E4 deduplication report artifact/table merged on the base) |
| Q0 Profile objects (`ProfileRequest`, `ProfilePolicy`, `DatasetProfile`, `ColumnProfile`, `ProfileReportArtifact`) | `missing` | No matching backend type, field, or accumulator exists in `backend/crates` | `missing` | `missing` |
| Q0 Quality objects (`QualityReport`, `QualityReportArtifact`, `QualityScoreVersion`) | `missing` | No matching backend type, field, or versioned formula exists in `backend/crates` | `missing` | `missing` |
| Q0 `Finding`, `FindingCategory`, `FindingEvidence` | `missing` | No typed Q0 finding with evidence/provenance exists; only `InspectionFinding` is defined | `missing` (`FindingEvidence` is additionally `blocked by E4` for row-level validation/dedup evidence on the base) | `missing` |

### 2.9 Existing logical rule vocabulary that can feed Q0

The repository already has `ValidationSeverity`, `Rule::Deduplicate`, and
`Rule::Validate` in `stillflow-plan` (`backend/crates/stillflow-plan/src/rule.rs`).
This is the precise fact boundary that Q0-D0 must record:

- **Logical contract vocabulary:** `implemented` — the rule variants, their
  fields, and validation rules are real definitions in `stillflow-plan`.
- **Engine runtime execution:** `unsupported on the inventory base` — the
  engine preflight, lowering, and prediction paths return
  `EngineError::UnsupportedRule` for both rules:
  `backend/crates/stillflow-engine/src/preflight.rs`,
  `backend/crates/stillflow-engine/src/lower.rs`,
  `backend/crates/stillflow-engine/src/predict.rs`.
- **Persistence/result summary:** `absent` — no E4 validation/dedup report
  artifact or storage table is merged on this base; PR #57 proposes these but is
  a read-only reference.

This matters for Q0 because duplicate and validation summaries are candidates
for Quality score evidence, but their runtime and persistence are blocked by E4
until E4-C0 and E4 runtime are approved.

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

- **Profile:** `missing`. No `profiles` table, no `ProfileReportArtifact`
  writer, no profile manifest or partition type.
- **Finding:** `missing` as persisted Q0 findings. `AssetMetadata.findings` are
  `runtime-only`; no `findings` table exists. E4 row-level `ValidationFinding` /
  `DuplicateFinding` artifacts are proposed in PR #57 but are not merged on this
  base.
- **QualityReport:** `missing`. No `quality_reports` table, no
  `QualityReportArtifact`, no report digest or provenance row.
- **Quality score:** `persisted` only as the opaque `snapshots.quality_score`
  metadata column (Section 3). No `QualityScoreVersion` record exists.

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

### 7.4 Frontend presentation-versus-backend classification

This is the required per-surface table. Classification values are scoped to the
frontend/product surface: `presentation-only` means the frontend carries labels,
types, or expected shapes without a backend contract; `mock` means values are
generated locally from sample data; `backend-backed` means a backend API/type
provides the surface on the inventory base. No row on this base is
`backend-backed`.

| Surface | Frontend evidence | Classification |
| --- | --- | --- |
| Profile | No `profile` component, page, label, or type exists in `src`; `PreviewColumn`/`DataPreviewResult` types exist but are unused by UI code | `presentation-only` (no rendered profile surface; no backend type) |
| Quality | `DetailPanel` displays `Quality Score` with `Good`/`Fair`/`Poor` labels from `metrics.qualityScore` (`src/components/DetailPanel.tsx`) | `mock` (local DuckDB-WASM computation) |
| Findings / issues | No findings/issues component or type exists in `src`; `ActivityPanel` renders only workspace event counts, not structured findings | `presentation-only` (no rendered findings surface; no backend finding API) |
| Distributions | No distribution component, type, or label exists in `src` | `presentation-only` (no rendered distribution surface) |
| Top values | No top-values component, type, or label exists in `src` | `presentation-only` (no rendered top-values surface) |
| Null | `DetailPanel` shows `Missing` percentage and `nullColumns`; `duckdb.ts` computes `missing` and `nullCount` from the sample tables; `PreviewColumn.nullCount` is declared but unused | `mock` (local computed and displayed) plus `presentation-only` for the unused `PreviewColumn.nullCount` type |
| Unique | `PreviewColumn.distinctCount` is declared in `src/types.ts` but not used by UI code; no unique metric is displayed | `presentation-only` (declared but unused type) |
| Duplicate | `DetailPanel` shows `Duplicates` percentage; `duckdb.ts` computes `duplicates` with `row_number()` for `deduplicate` nodes and sample data | `mock` (local DuckDB-WASM computation) |
| Quality score | `DetailPanel` displays `Quality Score`; `duckdb.ts` computes `100 - nullCount / (totalRows * 4) * 100`, clamped to `0..=100` | `mock` (local formula, not backend-supported) |

> S3 note (issue #79): the frontend symbols cited above — `ActivityPanel` (`src/components/ActivityPanel.tsx`) and the unused types `PreviewColumn` / `DataPreviewResult` in `src/types.ts` — were removed from `src` by the dead-code cleanup slice S3 (branch `agent/issue-079-s3-dead-code-cleanup`). The rows remain accurate as statements about their evidence base `main@89aab25`; they describe history, not current tree state.

## 8. Q-C0 missing object matrix

| Object / capability | Status on `main@89aab2551b8f73a32ed575bf75b3e3866b39d37c` | Evidence / gap |
| --- | --- | --- |
| `ProfileRequest` | `missing` | No request type, bounds, supported logical types, or sampling contract exists |
| `ProfilePolicy` | `missing` | No policy object exists for profile scope, bounds, supported types, sampling defaults, or result caps |
| `DatasetProfile` | `missing` | No dataset-level profile result/aggregate exists |
| `ColumnProfile` / `ColumnStatistics` | `missing` | No per-column metric types or accumulators exist |
| `ProfileReportArtifact` | `missing` | No artifact type, writer, digest, or storage table exists; generic artifact ownership is `blocked by E5` |
| `Finding` | `missing` | Only `InspectionFinding` exists; no typed Q0 finding with category/evidence/provenance |
| `FindingCategory` | `missing` | No schema/text/duplicate/privacy/distribution/leakage category enum |
| `FindingEvidence` | `missing` (blocked by E4) | No evidence object exists; E4 row-level `ValidationFinding`/`DuplicateFinding` evidence is proposed in PR #57 but not merged on this base |
| `QualityReportArtifact` | `missing` (blocked by E5) | No report artifact type, writer, digest, or provenance row exists; artifact ownership depends on E5 |
| `QualityScoreVersion` | `missing` | No versioned score formula, version field, or migration record exists; `quality_score` is opaque caller-injected metadata |
| Profile Job / Run integration | `blocked by E5` | No `Job`/`Run` type or repository exists on the base (E5-D0); profiling would require E5 Job/Run/Event ownership before Q-A1 |
| Profile Artifact read API | `blocked by E5` | No artifact read API exists; E5-A1 and E5 artifact ownership are not merged (backend `stillflow-api` is a placeholder) |
| `Rule::Validate` runtime execution | `blocked by E4` | Logical `Rule::Validate` vocabulary is implemented in `stillflow-plan`; engine preflight/lowering/prediction reject it as `UnsupportedRule` on this base |
| `Rule::Deduplicate` runtime execution | `blocked by E4` | Logical `Rule::Deduplicate` vocabulary is implemented in `stillflow-plan`; engine preflight/lowering/prediction reject it as `UnsupportedRule` on this base |
| Validation/Dedup report persistence | `blocked by E4` | E4 contract (PR #57) defines `ValidationReportArtifact`/`DeduplicationReportArtifact`; no such storage table or writer is merged on this base |
| Profile/Quality persistence | `missing` (blocked by E5 for artifact ownership) | No `profiles`, `findings`, or `quality_reports` table in `stillflow-storage`; artifact persistence depends on E5 ownership |
| Profile/Quality Job/API | `blocked by E5` | No job, run, status, cancel, or artifact-read API exists; E5-C0/E5-J1/E5-A1 are not merged |
| Reservoir/Random sampling | `placeholder` | `SamplingStrategy` enum exists; all connectors reject non-Head sampling |
| `InspectionFinding` | `implemented` (inspection only) | `AssetMetadata.findings` returned by inspect, not persisted |
| `WorkbookInspection` | `implemented` (structural only) | Region/header/formula analysis, not column data profiling |
| `quality_score` metadata field | `implemented` (opaque persisted) | `DatasetSnapshot` + SQLite `snapshots.quality_score` |
| `RequestContext` cancellation/deadline | `implemented` | Reusable by a future bounded profiler |
| Engine chunk/memory machinery | `implemented` | `largest_feasible_k`, `CanonicalRebatcher`, `MemoryTracker` |
| Snapshot publication pattern | `implemented` | Could inform artifact publication, but no generic artifact exists |

## 9. Q-C0 decisions pending freeze

The checklist currently marks Q-C0 as `blocked` until E5 Artifact/Run ownership
is stable (`docs/development/backend-completion-execution-checklist.md`).
The following table is the required Q-C0 decision/dependency table. It records
issues from #65 plus repository facts; none of the decisions are frozen by this
inventory.

| Decision input | Question to freeze | Current repository fact on `main@89aab2551b8f73a32ed575bf75b3e3866b39d37c` | Dependency / blocker |
| --- | --- | --- | --- |
| Exact vs sampled metrics | Are Q0 metrics exact on the bounded scan or sampled? | Only head/prefix preview sampling is implemented; all connectors reject `Reservoir`/`Random`; no profiler accumulators exist | Q-C0 |
| Deterministic sampling method/seed | Which deterministic seed/source is used if sampling is allowed? | `SamplingStrategy` enum exists; no deterministic seed field or sampling implementation exists | Q-C0; Q-R1 if sampling becomes real |
| Exact vs approximate distinct counts | Must distinct counts be exact, or may Q0 use approximate algorithms? | `PreviewColumn.distinctCount` exists only as an unused frontend type; no backend distinct accumulator exists (S3 note, issue #79: this type was removed from `src` by slice S3; row speaks to base `main@89aab25`) | Q-C0; operator-state limits |
| Top-K and histogram policies | What are the top-K and histogram bounds? | No backend top-values/histogram types or policies exist; no frontend surface exists | Q-C0 |
| Numeric summary and overflow behavior | Define min/max/mean/distribution and overflow behavior | No backend numeric accumulator or overflow policy exists | Q-C0 |
| Utf8/Binary length accounting | How are string/binary lengths and invalid values counted? | No backend length/invalid-value policy exists | Q-C0 |
| Null and invalid-value semantics | What counts as null vs invalid/empty? | Frontend-only `duckdb.ts` treats `NULL` and empty strings as missing; backend has no null metric policy | Q-C0; frontend formula is not backend-backed |
| Duplicate relationship to E4 exact dedup | How do Q0 duplicate metrics relate to E4 `Rule::Deduplicate`? | `Rule::Deduplicate` is logical-only on this base; engine rejects it; E4 contract proposes exact dedup and `DeduplicationReportArtifact` but is not merged | `blocked by E4` for runtime/evidence; Q-C0 for metric semantics |
| Core metrics definitions | Define row/column/null/unique/duplicate metric meanings | No Q0 metric types exist; `SnapshotStats` contains only row/byte/partition totals | Q-C0 |
| Finding categories | Define schema, text, duplicate, privacy, distribution, and leakage finding boundaries | No `FindingCategory` exists; `InspectionFinding` has only `code`, `message`, `severity` | Q-C0; `FindingEvidence` additionally `blocked by E4` |
| Quality score formula, version, missing-evidence | What formula, version, and missing-evidence behavior should Q-C0 freeze? | `quality_score` is opaque `Option<u8>`; no backend formula/version/evidence; frontend ad hoc formula is not backend-supported | Q-C0 |
| `ProfileRequest` and operator limits | Row, column, byte, memory, time, concurrency, operator-state, and supported logical type limits | `ReadRequest`, `BatchEnvelope`, `RequestContext`, engine memory/cancellation machinery exist and are reusable; no `ProfileRequest` exists | Q-C0 |
| Cancellation and deadline | How do Q0 runs honor cancellation/deadline? | `RequestContext` carries `CancellationToken` and `Option<Instant>`; materialize checks it before begin, per stream item, per chunk, and per append | Q-C0; existing engine machinery is reusable |
| Artifact provenance and canonical digest | What provenance and canonical digest do Profile/Quality artifacts carry? | No generic `ArtifactRef`/artifact writer exists; `SnapshotStore` has immutable snapshot publication; E5 artifact ownership not merged | `blocked by E5` for artifact ownership; Q-C0 for profile/quality digest definitions |
| Retention | What is the retention/lifecycle policy for Profile/Quality artifacts and reports? | No profile/quality artifact or report retention exists; no generic artifact lifecycle | `blocked by E5` |
| E5 Job/Run/Event integration | How do Profile/Quality runs integrate with E5 Job/Run/Event? | No `Job`/`Run`/generic event types exist on the base; `ExecutionEngine::materialize` is a direct async call, not a persisted run | `blocked by E5` (E5-C0/E5-J1/E5-A1) |
| Ownership boundaries | Which crates own Profile/Quality domain, execution, persistence, and API? | Non-binding candidates are listed in Section 10; accepted dependency direction is `api -> engine -> {plan, connectors, storage} -> core` | No new crate or dependency is frozen |
| Forbidden semantics | Which semantics must remain forbidden? | Checklist forbids LLM-defined metrics, opaque scores without evidence, and unbounded exact cardinality | Q-C0 |

## 10. Crate ownership facts

The accepted dependency direction for this repository is recorded in
`AGENTS.md` and is repeated here as the required Ownership and dependency fact:

```text
stillflow-api -> stillflow-engine
stillflow-engine -> stillflow-plan, stillflow-connectors, stillflow-storage
stillflow-plan -> stillflow-core
stillflow-connectors -> stillflow-core
stillflow-storage -> stillflow-core
stillflow-core -> no workspace crate
```

Equivalently, for Q0:

```text
api -> engine -> {plan, connectors, storage} -> core
```

No dependency may point from a lower layer back to a higher layer. The rows
below are current ownership facts; the list after the table is non-binding.

| Type / capability | Current owner crate | Evidence |
| --- | --- | --- |
| `InspectionFinding`, `FindingSeverity`, `AssetMetadata` | `stillflow-core` | `backend/crates/stillflow-core/src/domain/metadata.rs` |
| `WorkbookInspection`, region/header candidates | `stillflow-core` | `backend/crates/stillflow-core/src/domain/workbook.rs` |
| `SamplingStrategy`, `PreviewRequest`, `PreviewData` | `stillflow-core` | `backend/crates/stillflow-core/src/domain/preview.rs` |
| `DatasetSnapshot.quality_score`, `SnapshotStats` | `stillflow-core` | `backend/crates/stillflow-core/src/domain/snapshot.rs` |
| `RelationshipKind::Profiled` | `stillflow-core` | `backend/crates/stillflow-core/src/events/mod.rs` |
| `Rule`, `Rule::Validate`, `Rule::Deduplicate`, `ValidationSeverity` | `stillflow-plan` | `backend/crates/stillflow-plan/src/rule.rs` |
| `ExecutionIdentities.quality_score` | `stillflow-engine` | `backend/crates/stillflow-engine/src/lib.rs` |
| `MemoryTracker`, `CanonicalRebatcher`, chunk prediction | `stillflow-engine` | `backend/crates/stillflow-engine/src/memory.rs`, `remainder.rs`, `predict.rs` |
| `SnapshotDraft.quality_score`, SQLite snapshot metadata persistence | `stillflow-storage` | `backend/crates/stillflow-storage/src/manifest.rs`, `backend/crates/stillflow-storage/src/store.rs` |
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
