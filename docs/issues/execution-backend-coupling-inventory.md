# XR-D0 — current execution-backend coupling inventory

- **Base SHA:** `c0e828031f0141fa89e6b525b4314ebabd5f4f4e` (worktree `.worktrees/xr-d0-src`, verified `main@c0e8280`, "Merge pull request #53 from X44421/agent/issue-052-node-preview-runtime"). All citations below refer to this exact tree.
- **Target branch:** `agent/issue-094-execution-backend-coupling-inventory`
- **Date:** 2026-08-23
- **Scope:** Read-only, docs-only inventory of the current execution-backend coupling as it exists at the base SHA. This document is a factual input for XR-C0 / ADR-002. It designs nothing, proposes no API, no trait, no schema, and recommends no migration. Every material statement is labeled inline with exactly one of:
  - `[source fact]` — directly verifiable in the tree; cites `path:symbol`
  - `[accepted contract fact]` — stated by `docs/data-ingestion-architecture.md` or frozen AGENTS rules; cites doc section
  - `[static inference]` — deduced from code structure without executing it
  - `[requires experiment]` — cannot be settled without executable evidence
  - `[XR-C0 decision input]` — a fact that XR-C0/ADR-002 must weigh; not a recommendation

Citation shorthand: repo-root-relative paths; `path:symbol` means the named function/type/const in that file.

---

## 1. Public surface map

### 1.1 Crate inventory and dependency arrows (manifest evidence)

Workspace members: `stillflow-api`, `stillflow-connector-local-tabular`, `stillflow-connector-object-store`, `stillflow-connector-workbook`, `stillflow-connectors`, `stillflow-core`, `stillflow-engine`, `stillflow-plan`, `stillflow-storage`. [source fact] `backend/Cargo.toml:[workspace].members`

Locked versions: `polars 0.46.0`, `polars-arrow 0.46.0`, `arrow-array 59.2.0`, `arrow-schema 59.2.0`, `parquet 59.2.0`, `rusqlite 0.32.1`. [source fact] `backend/Cargo.lock`

Per-crate dependency lines (actual manifest content):

| Crate | Depends on (workspace crates) | Physical-engine deps | Evidence |
| --- | --- | --- | --- |
| stillflow-core | none | `arrow-array.workspace`, `arrow-schema.workspace` | `backend/crates/stillflow-core/Cargo.toml [dependencies]` |
| stillflow-plan | `stillflow-core.workspace` | none (no arrow, no polars) | `backend/crates/stillflow-plan/Cargo.toml [dependencies]` |
| stillflow-connectors | `stillflow-core.workspace` | none in `[dependencies]`; `arrow-array`/`arrow-schema` only under `[dev-dependencies]` | `backend/crates/stillflow-connectors/Cargo.toml` |
| stillflow-storage | `stillflow-core.workspace` | `arrow-array`, `arrow-ipc`, `arrow-schema` (all `.workspace = true`), `parquet.workspace`, `rusqlite.workspace` (`=0.32.1`, bundled) | `backend/crates/stillflow-storage/Cargo.toml [dependencies]` |
| stillflow-engine | `stillflow-connectors/core/plan/storage .workspace` | `arrow-array = { workspace = true, features = ["ffi"] }`, `arrow-buffer = "59"`, `arrow-data = "59"`, `arrow-schema = { workspace = true, features = ["ffi"] }`, `polars = { version = "0.46", default-features = false, features = ["lazy","strings","dtype-u8","dtype-u16","dtype-i8","dtype-i16","dtype-date","dtype-datetime","dtype-struct"] }`, `polars-arrow = "0.46"` | `backend/crates/stillflow-engine/Cargo.toml:10-27` |
| stillflow-connector-local-tabular | `stillflow-connectors.workspace`, `stillflow-core.workspace` | `arrow-array` (ffi), `arrow-cast`, `arrow-data = "59"`, `arrow-schema` (ffi), `arrow-select`, `polars = { version = "0.46", default-features = false, features = ["csv","dtype-date","dtype-datetime","dtype-struct","json","parquet"] }`, `polars-arrow = "0.46"` | `backend/crates/stillflow-connector-local-tabular/Cargo.toml:10-29` |
| stillflow-connector-object-store | `stillflow-connector-local-tabular.workspace`, `stillflow-connectors.workspace`, `stillflow-core.workspace` | `arrow-array/cast/schema/select`, `parquet = { workspace = true, features = ["async"] }`, `object_store` | `backend/crates/stillflow-connector-object-store/Cargo.toml` |
| stillflow-connector-workbook | `stillflow-connectors.workspace`, `stillflow-core.workspace` | `arrow-array`, `arrow-schema`, `arrow-select`, `calamine` | `backend/crates/stillflow-connector-workbook/Cargo.toml` |
| stillflow-api | `stillflow-core.workspace`, `stillflow-engine.workspace` | none | `backend/crates/stillflow-api/Cargo.toml [dependencies]` |

[source facts for all rows]

The declared direction matches the frozen rule set (`api -> engine -> {plan, connectors, storage} -> core`) both in the workspace comment `backend/Cargo.toml:24-25` and in AGENTS "Dependency direction". The adapter→adapter edge `stillflow-connector-object-store -> stillflow-connector-local-tabular` exists in the tree and is **not** covered by the frozen diagram, which lists only the contract crates. [source fact + XR-C0 decision input]

Two compile-time guards pin engine's adapter independence inside the test suite: `backend/crates/stillflow-engine/src/tests.rs:t19_engine_crate_does_not_depend_on_adapter_crates` asserts the engine manifest contains no adapter crate name, and `t20_engine_depends_on_core_plan_connectors_storage` asserts the four allowed dependencies. [source fact]

DuckDB: there is **no DuckDB dependency and no DuckDB identifier occurrence** in any backend code or manifest file (exhaustive case-insensitive sweep over all backend crates); the single in-backend mention is a prose doc comment, "Polars and DuckDB adapters lower these contracts in downstream crates", at `backend/crates/stillflow-plan/src/lib.rs:3`. DuckDB exists only as accepted-contract text (`docs/data-ingestion-architecture.md` §6.5, §17 Phase 1D) and frontend WASM context (§2). [source fact]

### 1.2 Public items involving LogicalPlan, BatchEnvelope, Snapshot, Verification, execution identities

`stillflow-plan` (logical only): `LogicalPlan{version,root,nodes}`, `PlanNode{inputs,kind}`, `PlanNodeKind::{Scan,Project,Filter,ApplyRules,Join,Union,Materialize}`, `JoinType`, `JoinKey`, `PlanNodeId`, `PlanFingerprint`, `PlanError`, `Rule` (10 variants incl. `Validate`, `Deduplicate`), `CastFailurePolicy`, `ValidationSeverity`, `PLAN_VERSION=1`, `PLAN_FINGERPRINT_ALGORITHM="stillflow-fnv1a64x4-v1"`. [source fact] `backend/crates/stillflow-plan/src/lib.rs:pub use plan/rule`; `backend/crates/stillflow-plan/src/plan.rs:PlanNodeKind`; `backend/crates/stillflow-plan/src/rule.rs:Rule`

`stillflow-core` public re-exports relevant to execution (`backend/crates/stillflow-core/src/lib.rs:pub use`):

- Batch boundary: `BatchEnvelope`, `BatchEnvelopeFactory`, `BatchError`, `LogicalSchemaFingerprint`, `BATCH_ENVELOPE_VERSION=1`, `MAX_BATCH_ROWS=65_536`, `MAX_BATCH_BYTES=64MiB`, `logical_schema_to_arrow`, `logical_schema_from_arrow`. [source fact]
- Snapshot domain: `DatasetSnapshot`, `SnapshotError`, `SnapshotStats`, `DATASET_SNAPSHOT_VERSION`. [source fact]
- Verification contract: `ArtifactKind`, `ArtifactProvenance(Draft/Input)`, `ArtifactSummary`, `ContentDigest=[u8;32]`, `InputRef/LogicalInputRef/SourceRowRef/RuleRef`, `VERIFICATION_CONTRACT_VERSION=1`, plus ~60 typed `ColumnId` constants for rejected/validation/dedup artifact sections. [source fact] `backend/crates/stillflow-core/src/verification.rs`
- Execution identities/requests: `ReadRequest` (with `MIN_BATCH_SIZE`/`MAX_BATCH_SIZE` bounds used by the engine), `RequestContext`, `BatchStream`, `BatchItem`. [source fact] `backend/crates/stillflow-core/src/domain/read.rs:ReadRequest`; `backend/crates/stillflow-core/src/request/mod.rs:RequestContext`
- Expressions/schemas: `Expr`, `ScalarValue` (exactly `Null|Boolean|Int64|UInt64|Float64(FiniteF64)|Utf8`), `BinaryOperator`, `UnaryOperator`, `SourceFilter`, `LogicalSchema/Field/Type`, `TimeUnit`, `ColumnId`. [source fact] `backend/crates/stillflow-core/src/expression.rs`; `backend/crates/stillflow-core/src/logical.rs`

`stillflow-engine` public surface (`backend/crates/stillflow-engine/src/lib.rs:pub use` + structs):

- `ExecutionEngine::new(registry)`, `::preflight(&LogicalPlan,&SourceConnection,&SourceAsset,Option<&LogicalSchema>,&RequestContext) -> PreparedPlan`, `::preview(PreviewRequest) -> PreviewResult`, `::materialize(ExecutionRequest) -> SnapshotManifest`. [source fact] `backend/crates/stillflow-engine/src/engine.rs:ExecutionEngine`
- `PreparedPlan` is exported but every field is `pub(crate)` — externally opaque. [source fact] `backend/crates/stillflow-engine/src/preflight.rs:PreparedPlan`
- `PreviewRequest{plan,target_node_id,connection,asset,schema_override,context,batch_size,row_limit,byte_limit}`; `PreviewResult{plan_fingerprint,target_node_id,schema,batches:Vec<BatchEnvelope>,rows_returned,bytes_returned,source_rows_scanned,source_bytes_scanned,source_rows_observed,source_bytes_observed,rows_truncated,bytes_truncated,scan_truncated,source_exhausted}`. [source fact] `backend/crates/stillflow-engine/src/lib.rs:PreviewRequest,PreviewResult`
- `ExecutionIdentities{snapshot_id,dataset_id,session_id,created_at,started_at,lineage:BTreeSet<Uuid>,quality_score:Option<u8>}` and `ExecutionRequest<'a>{plan,connection,asset,schema_override,identities,context,batch_size,store:&SnapshotStore}`. [source fact] `backend/crates/stillflow-engine/src/lib.rs:ExecutionIdentities,ExecutionRequest`
- Bound constants: `ENGINE_CONTRACT_VERSION=1`, `MAX_PLAN_NODES=64`, `MAX_RULES_PER_NODE=256`, `MAX_EXPR_NODES=1024`, `MAX_EXPR_DEPTH=64`, `MAX_LIVE_COLUMNAR_PAYLOADS=3`, `MAX_COMPILED_PLAN_BYTES=4MiB`, `MAX_FFI_SCRATCH_BYTES=1MiB`, `MAX_OPERATOR_STATE_BYTES=5MiB`, `MAX_ENGINE_PEAK_BYTES=3*64+5 MiB`, `MAX_ENGINE_CONCURRENT_RUNS=4`, `ENGINE_DEFAULT_DEADLINE=15min`, `ENGINE_MAX_DEADLINE=30min`, plus UTF8/int/float slot-size constants and the full preview ceiling set (`PREVIEW_*`). [source fact] `backend/crates/stillflow-engine/src/lib.rs:35-71`
- `EngineError` (18 variants incl. `UnsupportedOperator`, `UnsupportedRule`, `CastFailure`, `Arithmetic`, `SchemaDrift`, `Cancelled`, `Timeout`, `Busy`, `Ffi`). [source fact] `backend/crates/stillflow-engine/src/error.rs:EngineError`

`stillflow-connectors`: `SourceConnector` trait (`test_connection/discover/inspect/preview/read_batches/checkpoint`), `SourceConnectorRef`, `ConnectorRegistry`, `Capability`, `ConnectorCapabilities`, `RawBatchStream` (newtype over `BatchStream`). [source fact] `backend/crates/stillflow-connectors/src/lib.rs:pub use`; `backend/crates/stillflow-connectors/src/connector.rs:SourceConnector`

`stillflow-storage`: `SnapshotStore` (`open/begin_snapshot/load_manifest/read_batches/verify_snapshot/tombstone_snapshot/recover/collect_garbage`), `SnapshotWriter` (`append/commit`), `SnapshotBatchReader`, `SnapshotDraft/SnapshotManifest/SnapshotPartition/StorageLimits/RecoveryReport/GarbageCollectionReport`, verification-bundle writers (`VerificationBundleWriter`, `VerificationBundleDraft`, artifact types), `DedupIndex/DedupInsert`, `StorageError/IntegrityFailure`, `ContentDigest`. [source fact] `backend/crates/stillflow-storage/src/lib.rs:pub use`; `backend/crates/stillflow-storage/src/store.rs:SnapshotStore,SnapshotWriter`

`stillflow-api`: exports only `crate_name()`; routes are not implemented ("API routes are intentionally not implemented yet"). [source fact] `backend/crates/stillflow-api/src/lib.rs`

Adapter crates export only connector structs plus object-store access/credential helpers (`ObjectByteStream`, `ObjectInfo`, `ObjectStorageAccess`, `ObjectStoreCredentialResolver`, `S3CredentialMaterial`); every internal module (`bridge`, `read`, `schema`, `inspect`, `parquet`, …) is private. [source fact] `backend/crates/stillflow-connector-local-tabular/src/lib.rs:mod declarations`; same pattern in workbook/object-store `lib.rs`.

### 1.3 Do physical types escape into stable public APIs?

- **Polars: no escape found anywhere.** No public signature in any of the nine crates mentions a `polars` type. Polars usage is confined to private modules of `stillflow-engine` (`ffi`, `lower`, `types`, `remainder`, `predict`, `engine`, `preview`, `memory`, `tests`) and `stillflow-connector-local-tabular` (`read`, `inspect`, `schema`, `bridge`). [source fact] `backend/crates/stillflow-engine/src/lib.rs:mod list`; `backend/crates/stillflow-connector-local-tabular/src/lib.rs:14-23`
- **Raw Arrow does escape through `stillflow-core`:** `stillflow-core` depends on `arrow-array`/`arrow-schema` (`backend/crates/stillflow-core/Cargo.toml`), and `BatchEnvelope` stores a raw `RecordBatch` payload with public accessors `payload() -> &RecordBatch` and `into_payload() -> RecordBatch`; constructors `try_new`/`try_from_parts` take `RecordBatch`; `BatchEnvelopeFactory::arrow_schema()` exposes `&SchemaRef`; `logical_schema_to_arrow(schema) -> Result<SchemaRef,_>` is public. [source fact] `backend/crates/stillflow-core/src/batch.rs:BatchEnvelope.payload,BatchEnvelope.into_payload,BatchEnvelope.try_new,BatchEnvelopeFactory.arrow_schema,logical_schema_to_arrow`
  - Frozen rule context: AGENTS frozen rule 4 states "Public batches cross execution boundaries in a versioned `BatchEnvelope`; raw `RecordBatch` values remain an internal payload." ADR-001 "Execution plane" authorizes the envelope to carry "physical `RecordBatch` payloads". The observable state is: the envelope is public and versioned, and its `RecordBatch` is reachable through public functions of a stable crate. Whether that reachability satisfies "internal payload" is an interpretation question for XR-C0, recorded here without a verdict. [XR-C0 decision input]
- **Arrow escapes via storage/engine result types too:** `ExecutionEngine::materialize` returns `storage::SnapshotManifest` whose partitions reference Parquet artifacts; `SnapshotStore::read_batches` yields `SnapshotBatchReader` over Arrow record batches read from Parquet. [source fact] `backend/crates/stillflow-storage/src/store.rs:SnapshotStore.read_batches,write_envelope_parquet`
- **No DuckDB type can escape**: no DuckDB dependency exists. [source fact]

---

## 2. Physical coupling map

### 2.1 Complete Polars import/call-site enumeration

Every `use polars…` / `polars::…` hit site outside test code (test-only sites noted separately):

**stillflow-engine** (crate owns `polars 0.46` + `polars-arrow 0.46`):

1. `backend/crates/stillflow-engine/src/ffi.rs:20` — `use polars::prelude::DataFrame`; `ffi.rs:21-24` — `polars_arrow::ffi::{import_array_from_c, import_field_from_c, ArrowArray, ArrowSchema}`. Symbols:
   - `record_batch_to_dataframe(&RecordBatch) -> DataFrame` (`ffi.rs:49`) — **Arrow→Polars conversion point** via the C Data Interface: `export_arrow_array` (`ffi.rs:415`, uses `arrow_array::ffi::to_ffi`) then `import_into_polars` (`ffi.rs:431`, `import_field_from_c`/`import_array_from_c`/`Series::from_arrow`). Static layout asserts `size_of/align_of(PolarsArrowArray == FFI_ArrowArray)` etc. at `ffi.rs:29-32`.
   - `dataframe_to_record_batch(DataFrame, &LogicalSchema, &SchemaRef, &[(String,ScalarValue)]) -> RecordBatch` (`ffi.rs:92`) — **Polars→Arrow conversion point**, value-by-value through arrow builders: `column_to_arrow` (`ffi.rs:147`), `map_i8..map_u64/map_f32/map_f64` (`ffi.rs:191-241`), `bool_from_polars` (`243`), `date_from_polars` (`255`), `timestamp_from_polars` (`264`, applies timezone via `with_timezone_opt` at `308`), `utf8_from_polars_column` (`312`, handles scalar columns via `AnyValue` at `313-328`), `binary_from_polars_column` (`398`), deferred-literal arrays `array_from_literal`/`utf8_repeat` (`347`,`367`). List/Struct export is rejected: `EngineError::TypeError("list and struct execution is paused")` (`ffi.rs:170-172`). [source facts]
2. `backend/crates/stillflow-engine/src/lower.rs:1` — `col, lit, when, DataFrame, Expr as PolarsExpr, IntoLazy, NULL`. Symbols: `transform(frame,schema,steps)` (`lower.rs:12` — the lowering entry point), `apply_rule` (`48`), `names_for` (`236`), `field_name` (`240`), `lower_expr` (`247` — per-operator mapping incl. LUB strict casts at `270-284`, paused arithmetic/contains arms at `294-307`), `coalesce_exprs` (`345`), `literal` (`354`), `literal_scalar` (`365`, uses `AnyValue`/`Scalar`/`PlSmallStr`). Inline `polars::prelude::Column::full_null/new_scalar` at `lower.rs:116,130,142`. [source facts]
3. `backend/crates/stillflow-engine/src/types.rs:25` — `polars_data_type(&LogicalType) -> polars::prelude::DataType` (`types.rs:polars_data_type`; Timestamp unit mapping at `45-57`; Second unit and List/Struct rejected). The `DataType/TimeZone` import at `types.rs:28` is production code inside `polars_data_type`, not test-only (`types.rs` contains no `#[cfg(test)]` module). [source fact]
4. `backend/crates/stillflow-engine/src/engine.rs:16` — calls both ffi converters; `consume_envelope` (`engine.rs:330`) runs the cascade `record_batch_to_dataframe → lower::transform → dataframe_to_record_batch` inside `enter_phase(AllocatorPhase::Polars)` (`engine.rs:352`). [source fact]
5. `backend/crates/stillflow-engine/src/preview.rs:15` — imports the same two ffi fns; `lower_chunk` (`preview.rs:392`) repeats the cascade with shrink-retry (`n /= 2`) plus `tokio::task::yield_now()` between attempts (`preview.rs:442`). [source fact]
6. `backend/crates/stillflow-engine/src/memory.rs:9` — `AllocatorPhase::Polars` phase tag consumed by `set_alloc_phase`/`hold_polars` (`memory.rs:280`). [source fact]
7. `backend/crates/stillflow-engine/src/tests.rs` — Polars references are test names/assertions only (e.g., `t36_mid_schema_arrow_to_polars_import_failure_releases_all`, `t39_fails_before_polars_import`, `report.polars_phase_peak`); tests do not construct Polars frames directly except via the ffi path. [source fact]

**stillflow-connector-local-tabular** (crate owns its own `polars 0.46` decode-feature set + `polars-arrow 0.46`):

1. `backend/crates/stillflow-connector-local-tabular/src/read.rs:7-11` — `MmapBytesReader`; `CsvReadOptions, DataFrame, JsonFormat, JsonReader, ParallelStrategy, ParquetReader, SerReader`. Call sites: CSV decoder field `decoder: polars::prelude::OwnedBatchedCsvReader` (`read.rs:CsvState.decoder`, line 66), drained via `reader.decoder.next_batches(1)` (`read.rs:377`); JSON projection re-encodes selected fields to NDJSON and decodes with `JsonReader…JsonLines.with_schema` (`read.rs:~465-477`); Parquet decode via `ParquetReader::new(file)...finish()` (`read.rs:202,217,404,411`); metadata holder `metadata: polars::io::parquet::metadata::FileMetadataRef` (`read.rs:ParquetState.metadata`, line 74); error adapters `polars_open_error`/`polars_data_error` (`read.rs:966,974`). [source facts]
2. `backend/crates/stillflow-connector-local-tabular/src/inspect.rs:4` — `ParquetReader, SerReader`; `inspect_opened_asset` reads Parquet schemas for inference (`inspect.rs:26-34`). [source fact]
3. `backend/crates/stillflow-connector-local-tabular/src/schema.rs:16,104,121` — `logical_schema_from_polars_arrow`, `polars_schema_from_logical`, `polars_type_from_logical`; time-unit conversions `logical_time_unit`/`polars_time_unit` (`schema.rs:225,234`). These convert between Polars-arrow field types and `LogicalType`. [source fact]
4. `backend/crates/stillflow-connector-local-tabular/src/bridge/mod.rs:14` — `CompatLevel, DataFrame`. `dataframe_to_record_batch(frame,…)` (`bridge/mod.rs:19`) — **Polars→Arrow conversion point**: `frame.rechunk_to_record_batch(CompatLevel::oldest())` then C-Data-Interface import `ffi::import_array` + `make_array`, optional normalization via `arrow_cast::cast` (`bridge/mod.rs:49-78`), nested required-value validation (`validate_required_values`/`validate_nested_values`, `92`/`124`). [source fact]
5. `backend/crates/stillflow-connector-local-tabular/src/bridge/ffi.rs` — `polars_arrow` FFI import into arrow-rs types (module `#[allow(unsafe_code)]`). [source fact]

No other backend crate references polars in non-test code (workbook, object-store, connectors, plan, core, storage, api: zero hits). [source fact]

### 2.2 Arrow↔Polars conversion points (summary)

| Direction | Site | Mechanism | Caller |
| --- | --- | --- | --- |
| Arrow 59 → Polars 0.46 | `backend/crates/stillflow-engine/src/ffi.rs:record_batch_to_dataframe` | `to_ffi` export → `polars_arrow::ffi` import (C ABI, zero-copy handoff) | `engine.rs:consume_envelope`, `preview.rs:lower_chunk` |
| Polars → Arrow 59 (engine) | `backend/crates/stillflow-engine/src/ffi.rs:dataframe_to_record_batch` | per-value iteration into arrow-rs builders (copying, canonical reconstruction) | same two callers |
| Polars → Arrow 59 (connector) | `backend/crates/stillflow-connector-local-tabular/src/bridge/mod.rs:dataframe_to_record_batch` | `rechunk_to_record_batch` → C ABI import → optional `arrow_cast::cast` | local-tabular CSV/JSON/Parquet readers |
| Arrow 59 → Polars (connector) | none | — | — |

[source facts for all rows] There is exactly one engine-side import funnel and one engine-side export funnel; the connector side has its own independent bridge with a different mechanism (FFI-based export vs builder-copy). [static inference from the enumeration above]

### 2.3 Ownership of each execution stage

| Stage | Owning crate/file | Entry symbol | Uses Polars? |
| --- | --- | --- | --- |
| Logical planning/validation/fingerprint | `stillflow-plan` | `backend/crates/stillflow-plan/src/plan.rs:LogicalPlan.validate,canonical_bytes,fingerprint`; `rule.rs:Rule.validate` | No (crate has no physical deps) |
| Preflight / compilation to steps | `stillflow-engine` | `preflight.rs:preflight` → `PreparedPlan{push_projection,scan_projection,expected_connector,scan_output,materialize_schema,steps,target_steps,target_schema,materialize_id}`; step IR `CompiledStep::{Project,Filter,Rules}` | No |
| Scan binding + source authorization | `stillflow-engine` | `preflight.rs:bind_scan`, `authorized_source_schema` (calls `ConnectorRegistry::inspect`) | No |
| Connector read streaming | `stillflow-connectors` + adapters | `registry.rs:ConnectorRegistry::read_batches`; local-tabular `read.rs:prepare_reader` (Polars decoders); object-store `parquet.rs` (parquet crate reader); workbook `read.rs` (arrow builders over calamine) | Yes (local-tabular only) |
| Chunk sizing before Polars | `stillflow-engine` | `predict.rs:largest_feasible_k,predict` (predicted physical byte model) | No (operates on Arrow arrays + logical types) |
| Lowering (rules/exprs → execution) | `stillflow-engine` | `lower.rs:transform,apply_rule,lower_expr` | Yes — the only rule executor |
| Rebatching to canonical envelopes | `stillflow-engine` | `remainder.rs:CanonicalRebatcher` (+ `ColumnSink` family, `max_prefix`, `append_rows`, `flush`) | No — pure arrow-rs builders |
| Memory accounting | `stillflow-engine` | `memory.rs:MemoryTracker`, `AllocatorPhase`, `live_payload_guard` (`error.rs`) | Phase-tagged around Polars |
| Materialization/publication | `stillflow-storage` | `store.rs:SnapshotWriter.append/commit`, `write_envelope_parquet`, `install_partitions`, `commit_manifest` | No (parquet + rusqlite) |
| Verification flow | `stillflow-storage` (+ contracts in core) | `store.rs:SnapshotStore.verify_snapshot`, `digest.rs:digest_file`; bundle writers `bundle.rs:VerificationBundleWriter` | No |
| Node preview orchestration | `stillflow-engine` | `preview.rs:preview` (pub(crate)), exposed as `ExecutionEngine::preview` | Yes (via lower/ffi) |

[source facts for all rows]

Notable absences (verified by grep, stated explicitly because they matter):

- Nothing outside `stillflow-storage` calls `verify_snapshot`, `recover`, or `VerificationBundleWriter`; the engine's materialize path never invokes them today (only `begin_snapshot`/`append`/`commit`). [source fact — absence]
- `LogicalPlan::fingerprint()` is computed only in the preview path (`preview.rs:90-93`). The materialize path never computes a plan fingerprint, and `DatasetSnapshot` carries `schema_fingerprint` but **no plan digest field** (`domain/snapshot.rs:DatasetSnapshot`, fields listed at `54-71`). Plan-digest constants exist only for future E4 artifact columns (`verification.rs:REJECTED_PLAN_FINGERPRINT_COLUMN_ID` etc.). [source fact]
- Engine passes `filter: None` in every `ReadRequest` it builds (`engine.rs:274`, `preview.rs:103`); the `SourceFilter` pushdown channel of `ReadRequest.filter` (`domain/read.rs:20`) is currently unused by the engine. Projection pushdown IS used when `Capability::ColumnProjection` holds (`preflight.rs:132`, `engine.rs:271-273`). [source fact]

---

## 3. Operator ownership matrix

Legend — Supported state evidence cites the enforcing symbol; "resource law" cites where the bound lives.

| Operator (logical owner) | Current physical executor | Supported / rejected (evidence) | Resource law (if any) | Output/artifact owner |
| --- | --- | --- | --- | --- |
| **Scan** — `PlanNodeKind::Scan` (`stillflow-plan/src/plan.rs`) | `ConnectorRegistry::read_batches` → adapter stream wrapped by `attach_request_context` (`connectors/src/raw_batch_stream.rs`, `core/src/stream/mod.rs:attach_request_context`); consumed in `engine.rs:stream_and_publish` | Supported for exactly one Scan; multi-scan rejected (`preflight.rs:linearize` — "exactly one scan and one materialize"); SQL/document kinds rejected (`preflight.rs:reject_phase_kinds`); `Streaming` capability required (`preflight.rs:129-131`) | Envelope caps `MAX_BATCH_ROWS=65_536`/`MAX_BATCH_BYTES=64MiB` enforced at construction (`core/src/batch.rs:BatchEnvelopeFactory.try_build`); schema drift aborts run (`engine.rs:SchemaDrift` check at `301-307`) | Envelopes produced by connector via `BatchEnvelopeFactory`; output sequence re-assigned by engine rebatcher |
| **Project** — `PlanNodeKind::Project` | Dual path: pushdown to connector when `Capability::ColumnProjection` (`preflight.rs:132`, `ReadRequest.projection` in `engine.rs:271`); otherwise in-engine `CompiledStep::Project` → Polars `frame.select` (`lower.rs:transform` Project arm, `22-29`) | Supported (`preflight.rs:161-169`) | Duplicate projection columns rejected (`preflight.rs:project_schema`) | Schema produced by `preflight.rs:project_schema`; column order = projection order |
| **Filter** — `PlanNodeKind::Filter` | In-engine only: `CompiledStep::Filter` → Polars lazy `filter` (`lower.rs:30-37`). Source-side predicate pushdown NOT performed (`ReadRequest.filter: None` at `engine.rs:274`) | Supported; scan-predicate expr validated then executed post-scan (`preflight.rs:145-151`) | Predicate must type-check Boolean (`typing.rs:require_boolean`); iterative node/depth caps `MAX_EXPR_NODES/MAX_EXPR_DEPTH` (`preflight.rs:validate_expr_iterative`) | None (row subset; schema unchanged per `propagate_schema`) |
| **ApplyRules** — `PlanNodeKind::ApplyRules` | Per-rule interpreter `lower.rs:apply_rule` on a Polars `DataFrame` chunk (Rename/DropColumn/Trim/DeriveColumn/ReplaceLiteral/FillNull/Cast/FilterRows execute; Validate/Deduplicate rejected earlier) | Partially supported: 8 of 10 rules; `Rule::Validate`/`Rule::Deduplicate` → `EngineError::UnsupportedRule` in preflight (`preflight.rs:186-200`) and defense-in-depth in lowering (`lower.rs:225-232`) | Rule count `1..=256` (`preflight.rs:180-184`); working-set cap `MAX_BATCH_BYTES` per Polars chunk (`engine.rs:358`, `memory.rs:hold_polars`) | Working `LogicalSchema` updated per rule (`preflight.rs:apply_rule_schema`); deferred literal columns tracked in `lower.rs:transform` return |
| **Validate** (as `Rule::Validate`, `stillflow-plan/src/rule.rs:64`) | None — no executor exists | Rejected in this delivery phase (`preflight.rs:187-191`, `lower.rs:225-228`); E4 scope per architecture doc §17 Phase 1C ("E4: Validate, Rejected Rows, Deduplicate") [accepted contract fact: data-ingestion-architecture.md §17 Phase 1C] | Unknown — not defined beyond rejection; issue #54 contract exists but is unimplemented in this tree | Future artifact owners exist as dormant contracts: `core/src/verification.rs` column-ID constants; `storage/src/artifact.rs` section schemas; `bundle.rs` writers — none wired to any caller [source fact — absence] |
| **Deduplicate** (as `Rule::Deduplicate`, `rule.rs:61`) | None — no executor exists | Rejected identically (`preflight.rs:193-198`, `lower.rs:229-232`) | Unknown — unimplemented; storage-side `dedup.rs:DedupIndex` exists but has no engine caller | Same dormant-contract state as Validate |
| **Join** — `PlanNodeKind::Join` | None | Rejected pre-stream: `UnsupportedOperator` for any Join node (`preflight.rs:63-67`); contract §10.6 and architecture doc §17 ("Phase 1C does not execute `Join` / `Union`, SQL connectors, DuckDB") [accepted contract fact: issue-046 §10.6; data-ingestion-architecture.md §17] | n/a | n/a; join-key expressions still shape-validated (`preflight.rs:validate_plan_exprs_iterative` Join arm) |
| **Union** — `PlanNodeKind::Union` | None | Rejected identically (`preflight.rs:63-67`) | n/a | n/a |
| **Materialize** — `PlanNodeKind::Materialize` | Identity transform on rows/schema in engine + publication via `SnapshotWriter`: one Parquet partition per non-empty output envelope (`store.rs:SnapshotWriter.append_inner` → `write_envelope_parquet`, SNAPPY, row-group ≤ `MAX_BATCH_ROWS`), atomic commit (`store.rs:SnapshotWriter.commit` → `install_partitions` rename + SQLite `commit_manifest`) | Supported exactly once per plan, as root (`preflight.rs:linearize` root check; duplicate-materialize rejected `preflight.rs:213-215`) | `output_label` must be non-empty + secret-safe (`preflight.rs:validate_output_label` using `Expr::validate_shape`); storage limits from `manifest.rs:StorageLimits` (envelopes/partitions/rows/stored bytes) | `stillflow-storage` owns staging dir, immutable partitions, checksummed manifest, visibility (`insert_visible_snapshot`) |

Unknown cells, marked explicitly:

- Whether scan predicates will ever be pushed down, and under which capability gate, is not decided anywhere in the tree (the channel exists, the engine never sets it). [unknown — recorded as XR-C0 decision input]
- Resource laws for Validate/Deduplicate/Join/Union are undefined in executable terms; only typed rejection exists. [unknown]
- No cell above claims performance or equivalence characteristics; none is measurable from this tree. [scope note]

---

## 4. Semantic dependency matrix

Each claim is classified as: **SF** = StillFlow contract ([accepted contract fact], citing doc/contract section), **P** = Polars implementation behavior, **A** = Arrow physical behavior, **S** = storage behavior, **U** = unknown-requires-experiment.

| Area | What the tree shows | Classification |
| --- | --- | --- |
| NULL logic — three-valued And/Or | Contract: "`And`/`Or` use three-valued Boolean logic already implied by Polars nulls"; Filter keeps only `true` (issue-046 §11.3, §10.3). Implementation delegates to Polars `and`/`or`/`filter` (`lower.rs:285-293,30-37`) | SF (statement) + P (execution). The concrete truth tables are whatever Polars 0.46 produces; no test pins e.g. `null AND false`. U for cross-engine equivalence |
| NULL logic — comparisons | `eq/neq/lt/...` lowered directly (`lower.rs:286-291`); comparable pairs gated by `least_upper_bound` (`typing.rs:comparable_pair`, `logical.rs:least_upper_bound`). Null-vs-value comparison outcome is not asserted by any test found | P + U (requires experiment to document actual drop/null behavior per operator) |
| NULL logic — FillNull/ReplaceLiteral-to-null | Contract: FillNull requires non-null value and forces `nullable=false`; ReplaceLiteral `to=Null` forces `nullable=true` (issue-046 §11.2). Implemented in `preflight.rs:apply_rule_schema` (`597-639`) and executed via `fill_null`/`when(...)` (`lower.rs:169-194`) | SF + P |
| Casts — policy mapping | `CastFailurePolicy::Error` → `strict_cast`; `SetNull` → lenient `cast` (`lower.rs:202-206`); failure surfaced as `EngineError::CastFailure` without cell values (contract issue-046 §11.5; error variant `error.rs:CastFailure`). Which inputs fail strict cast = Polars behavior | SF (policy) + P (failure set) |
| Casts — paused directions | Date32/Timestamp→Utf8 paused (unsound byte ceilings); binary casts rejected; Timestamp-second unit paused; List/Struct execution paused — enforced in `typing.rs:reject_paused_expr/reject_paused_type` (`23-89`), `preflight.rs:reject_paused_cast` (`691-707`), mirrored in `types.rs:polars_data_type` and `ffi.rs:column_to_arrow` | SF (rationale in issue-046 §11.5) + enforcement is SF-level engine behavior [source fact] |
| Arithmetic overflow/divide-by-zero | Entire Add/Subtract/Multiply/Divide/Modulo/Negate surface is **paused** at preflight and lowering (`typing.rs:52-60,181-187`; `lower.rs:294-302,255-262`) pending checked semantics. The issue-046 §11.5 divide/modulo/overflow law (truncate-toward-zero integer division, IEEE float division, `EngineError::Arithmetic`) is therefore *contract text with no executing implementation* in this tree | SF (law) + [source fact — absence of implementation]; the eventual numeric semantics will be whichever executor implements them: U |
| NaN / infinities | Float literals must be finite (`FiniteF64`, `expression.rs:9-20`; preflight guard `preflight.rs:780-784`). Non-finite results cannot arise from paused arithmetic. Behavior of stored NaN payloads / signed zeros across the FFI bridge and Parquet round-trip is untested and undocumented | A/P boundary + U (requires experiment: NaN bit-pattern and −0.0 survival through `ffi.rs` and `write_envelope_parquet` is not evidenced) |
| Unicode/string behavior — Trim | Contract: Polars default Unicode whitespace strip, interior whitespace untouched, null stays null (issue-046 §11.2). Implemented as `str().strip_chars(NULL)` (`lower.rs:87-99`). Exact codepoint set = Polars 0.46 `strip_chars` semantics; no fixture enumerates it | SF (intent) + P (codepoint set) + U (no pinned fixture) |
| String matching | `Contains` paused until the regex feature is approved (`typing.rs:46-50`; `lower.rs:303-307`); ReplaceLiteral defined as exact scalar equality, not regex/collation-insensitive (issue-046 §11.2) | SF + [source fact — pause] |
| String ordering | Ordered comparisons restricted to numerics/Date32/Timestamp by `ordered_pair` (`typing.rs:211-235`) — Utf8 `<` can never reach the executor in v1 plans | SF-level engine restriction [source fact] |
| Timezone | `Timestamp{unit,time_zone}` carried end-to-end: `types.rs:polars_data_type` maps to Polars `Datetime(unit, TimeZone)`; export re-attaches tz string via `with_timezone_opt` (`ffi.rs:305-310`); mixed-timezone timestamps have no LUB so are incomparable (`logical.rs:least_upper_bound` Timestamp arm requires equal timezone, `126-135`). Test `t48_timestamp_timezone_retention` covers `"UTC"` only | SF/LUB + P (retention mechanics) + U (non-UTC zone strings untested; DST-bearing zones untested) |
| Ordering / row order | Contract: operators must not sort/shuffle/sample/hash-aggregate; Scan row order = Snapshot row order (issue-046 §10 preamble). Implementation: no sort/shuffle operator exists in `PlanNodeKind`; rebatcher appends strictly in arrival order (`remainder.rs:push_with_base` loop); Polars `select/filter/with_column` order preservation is relied upon but is itself Polars behavior | SF (law) + P (mechanism). Cross-partition equality tested: `t02_two_input_partitionings_yield_equal_rows_and_stats`, `t03_fixed_batch_size_yields_equal_output_envelope_boundaries` |
| Partition / batch boundaries | Contract: split envelope into execution chunks *before* Polars; remainder→envelope is move/freeze, never a second copy (issue-046 §14, §14.1). Implementation: `predict.rs:largest_feasible_k` picks k from predicted bytes; `remainder.rs:max_prefix` binary-searches an admission prefix against exact post-append bytes; freeze conditions `rows >= pack_limit || remainder_bytes >= MAX_BATCH_BYTES` (`remainder.rs:168`); output sequences assigned 0.. at flush (`remainder.rs:flush` `next_sequence`) | SF + [source fact]. Storage side: one partition per non-empty appended envelope, SNAPPY, row-group ≤ `MAX_BATCH_ROWS` (`store.rs:write_envelope_parquet`) = S |
| Storage format fidelity | Parquet encode/decode of canonical Arrow schema incl. fingerprint metadata; reader rebuilds batches via `ParquetRecordBatchReaderBuilder` (`store.rs:12-15`, `read_partition`) | S + A. Whether every LogicalType round-trips Parquet bit-exactly is exercised by storage tests, not by a cross-engine differential harness: U for equivalence claims |

No statement in this table asserts performance, portability, or cross-engine equivalence; those would require executable evidence that does not exist in the tree. [scope note]

---

## 5. Runtime authority map

Where each responsibility lives today, and whether it sits above the physical-executor boundary (the Polars call inside `ffi.rs`/`lower.rs`):

| Responsibility | Lives in (symbols) | Above executor boundary? |
| --- | --- | --- |
| Cancellation | `core/src/request/mod.rs:RequestContext.cancellation` (tokio `CancellationToken`); polled by `ensure_active` between envelopes/chunks/attempts: `engine.rs:175,266,299,343`, `preview.rs:141,198,401`; connector-side enforcement `core/src/stream/mod.rs:CancellableBatchStream.poll_next`, `attach_request_context` | Above, but coarse: checks happen *between* synchronous Polars cascades; nothing interrupts a running `collect()` [source fact for placement + static inference for the overshoot window; magnitude requires experiment] |
| Deadline handling | `RequestContext.deadline/remaining/ensure_active` (`request/mod.rs:46-69`); admission caps `ENGINE_DEFAULT_DEADLINE/ENGINE_MAX_DEADLINE` (`engine.rs:materialize_inner` `169-190`), `PREVIEW_DEFAULT_DEADLINE/PREVIEW_MAX_DEADLINE` (`preview.rs:31-73`); cooperative yield between shrink-retries `preview.rs:tokio::task::yield_now` (`442`) | Same profile as cancellation [source fact + static inference] |
| Concurrency model | Single shared `tokio::sync::Semaphore` run-gate, `MAX_ENGINE_CONCURRENT_RUNS=4`, `try_acquire_owned` → `EngineError::Busy` (`engine.rs:ExecutionEngine.new/run_gate`, `103-107`, `192-195`; preview takes the same permit `preview.rs:75-78`). Matches contract "Preview shares the E2 run gate" (data-ingestion-architecture.md §12) [accepted contract fact] | Above |
| Memory / live-payload bounds | `memory.rs:MemoryTracker` (`hold_envelope/drop_envelope/hold_polars/drop_polars/hold_incoming/hold_remainder/pre_check_realloc_peak`), live-payload guard ≤ `MAX_LIVE_COLUMNAR_PAYLOADS=3` (`error.rs:live_payload_guard`), peak ceiling `MAX_ENGINE_PEAK_BYTES` (`lib.rs:44-45`), per-phase global counters `POLARS_LIVE/POLARS_PEAK/...` (`memory.rs:49-57`), storage-append phase recorded but excluded from ceiling (`memory.rs:refresh/report` comments citing Issue #46 T23/T44). Law text: issue-046 §14.1 ceiling table [accepted contract fact + source fact] | Straddles: admission decisions are above; the phase tags wrap the Polars region itself (`engine.rs:352`, `remainder.rs:137,190`) |
| Retry / fallback | No automatic retry loop exists for runs. `EngineError::retryable()` classifies Timeout/Busy/storage-Busy/connector-retryable as retryable (`error.rs:83-90`); sanitized summaries force an Internal fallback when sanitization fails (`error.rs:sanitized_summary`, fallback `224-228`). The preview export shrink-retry (`n/=2` halving, `preview.rs:426-443`) is bounded adaptation inside one chunk, not run-level retry | Above |
| Execution identity | Injected `ExecutionIdentities`; nil-UUID/quality-score/temporal-order validation `engine.rs:validate_identities` (`396-418`); engine is forbidden from generating IDs/timestamps (issue-046 §11.1) [accepted contract fact]; enforced by tests `t21_injected_identities_appear_unchanged_in_manifest`, `t22_engine_does_not_call_uuid_new_v4_or_utc_now_on_materialize_path` | Above |
| Plan digest / fingerprint | `stillflow-plan`: `plan.rs:LogicalPlan.canonical_bytes/fingerprint` (`stillflow-fnv1a64x4-v1`). Consumed only by preview (`preview.rs:90-93` → `PreviewResult.plan_fingerprint`). Materialize computes none; `DatasetSnapshot` persists `schema_fingerprint` only (`domain/snapshot.rs:DatasetSnapshot`) | Above (where present); the absence on the materialize path is itself a fact XR-C0 needs [XR-C0 decision input] |
| Verification flow | Contracts in `core/src/verification.rs` (`VERIFICATION_CONTRACT_VERSION=1`, provenance drafts, artifact column IDs). Execution: `storage/src/store.rs:SnapshotStore.verify_snapshot` re-validates partitions/checksums; `digest.rs:digest_file` (SHA-256 per partition, called during append at `store.rs:write_envelope_parquet`). Bundle/report writers (`bundle.rs:VerificationBundleWriter`, `artifact.rs` section schemas) exist with no engine/API caller in this tree [source fact — absence] | Beside/below: entirely inside storage today |
| Recovery | `store.rs:SnapshotStore.recover` and garbage collection `collect_garbage`; activity guards `acquire_activity`/`acquire_maintenance`; `RecoveryReport`/`GarbageCollectionReport` (`manifest.rs`) | Below (storage-internal); unused by engine |
| Atomic publication | Staging directory → `install_partitions` (fsync'd renames) → SQLite manifest commit → visibility insert (`store.rs:SnapshotWriter.commit`, `create_final_snapshot_directory`, `insert_visible_snapshot`); abort-on-drop `impl Drop for SnapshotWriter` removes staging/installed dirs and aborts publication; engine drops the writer on any stream error before commit (`engine.rs:run_with_permit` match arms `244-255`). Invariant: readers never observe partial snapshots (ADR-001 invariant 6) [accepted contract fact: adr-001 §Invariants] | Below |
| Error taxonomy/sanitization | `EngineError.category()/retryable()/sanitized_summary()` (`error.rs`), secret-shape gates `Expr::validate_shape` reused for labels/literals (`preflight.rs:validate_output_label`), core `ensure_no_secret_fields` | Above |

---

## 6. Test/evidence map

### 6.1 Contract-law tests vs Polars-specific regression tests

All in `backend/crates/stillflow-engine/src/tests.rs` unless noted (53 numbered tests total; t26-t29 are unassigned in the numbering sequence). Classification below is [static inference] from what each test asserts, applied consistently:

**Contract-law character (hold regardless of which executor sat under `lower.rs`):**

- Dependency direction: `t19_engine_crate_does_not_depend_on_adapter_crates`, `t20_engine_depends_on_core_plan_connectors_storage` (manifest-text assertions).
- Operator support/rejection: `t04_join_preflight_is_unsupported_operator`, `t05_union_preflight_is_unsupported_operator`, `t06_validate_and_deduplicate_preflight_is_unsupported_rule`, `t30_materialize_rejects_join_with_stale_prepared_plan`.
- Identity authority: `t21_injected_identities_appear_unchanged_in_manifest`, `t22_engine_does_not_call_uuid_new_v4_or_utc_now_on_materialize_path`.
- Determinism across partitionings / batch-boundary law: `t02_two_input_partitionings_yield_equal_rows_and_stats`, `t03_fixed_batch_size_yields_equal_output_envelope_boundaries` (asserts literal partition sizes `[6,6,6,2]`).
- Failure isolation / no partial publication: `t08_connector_schema_drift_aborts_and_publishes_nothing`, `t09_cancel_before_read_batches_publishes_nothing`, `t10_cancel_during_lowering_publishes_nothing`, `t11_cancel_after_append_before_commit_publishes_nothing`, `t12_deadline_before_commit_publishes_nothing`, `t31_missing_schema_override_cancelled_context_fails_before_inspect`, `t25_empty_source_commits_zero_row_snapshot`.
- Typed-error law: `t16_unknown_column_id_is_unknown_column`, `t17_incomparable_expr_types_are_type_error`, `t34_arithmetic_paused_fails_fast_in_preflight`, `t40_error_category_and_retryability_mapping`, `t54_fallback_error_sanitization_is_always_internal`, `t49_iterative_ast_guard_rejects_deep_expression_fast`.
- Schema-propagation law: `t33_replace_literal_with_to_null_makes_field_nullable`, `t51_typed_null_derivation`, `t50_lub_strict_casting_in_comparisons_and_coalesce`, `t45_date_to_utf8_is_type_error`, `t53_binary_cast_rejection`, `t32_no_column_projection_scan_output_is_projected`, `t35_secret_like_output_label_is_invalid_plan`.

**Executor-coupled regression character (assert outcomes of the current Polars 0.46 + FFI arrangement):**

- `t13_cast_error_fails_without_embedding_cell_sentinel`, `t14_cast_set_null_writes_null_and_continues`, `t15_rules_trim_replace_fill_drop_rename_derive_filter_match_golden` (the single end-to-end golden), `t48_timestamp_timezone_retention` — these pin concrete transformed values produced through `ffi.rs`+`lower.rs`.
- `t36_mid_schema_arrow_to_polars_import_failure_releases_all`, `t39_fails_before_polars_import` — exercise the Polars FFI import failure path specifically.
- Memory-law tests measured through the phased allocator around real Polars work: `t23_peak_live_payloads_and_engine_bytes_streaming`, `t37_derive_wide_utf8_chunks_before_polars`, `t38_replace_literal_and_fill_null_2kib_strings_over_65536_rows`, `t41_split_envelope_keeps_remainder_with_polars`, `t42_derive_then_drop_then_trim_and_replace_uses_predicted_table`, `t43_utf8_byte_cap_uses_offset_overhead`, `t44_phased_allocator_excludes_storage_encode`, `t46_near_64mib_export_transition_respects_bounds`, `t47_4096_columns_no_pack_limit_bulk_preallocation`, `t52_float_to_utf8_prediction_bound`, `t55_near_64mib_nullable_int64_remainder_freeze_respects_bounds`, `t56_near_60mib_nullable_boolean_remainder_freeze_respects_bounds`, `t57_all_valid_flush_then_nullable_flush_resets_validity`, `t24_fifth_concurrent_materialize_is_busy`.
- Preview estimator unit tests: `#[cfg(test)] mod estimator_tests` in `preview.rs:618+`.

This split is a reading of assertion content, not a claim about intent; the test file itself does not carry the classification. [static inference]

### 6.2 Other suites

- `backend/crates/stillflow-core/src/serde_tests.rs` — serialization round-trips incl. secret-free connection payloads (contract-law).
- `backend/crates/stillflow-storage/src/*.rs` inline `#[cfg(test)]` modules — publication atomicity, recovery, dedup index, artifact sections (storage-behavior law).
- Integration suites: `connector-local-tabular/tests/local_tabular.rs` + `memory_bound.rs`, `connector-object-store/tests/object_store_connector.rs`, `connector-workbook/tests/workbook_connector.rs` (connector boundedness/memory laws).

### 6.3 Differential / golden coverage that exists

- One golden end-to-end rule pipeline: `t15` (fixed input rows → expected output rows after trim/replace/fill/drop/rename/derive/filter).
- Two within-engine differential tests varying envelope partitioning and batch size: `t02`, `t03`.
- Preview flag-completion matrix assertions embedded in preview tests (truncation-flag co-occurrence rules per issue #50 §9.3 are enforced by `preview.rs:335-345` internal consistency checks and tests referencing them).

[source facts]

### 6.4 Evidence gaps XR-C0 should know about (facts only; no proposed remedy)

1. No second executor, simulator, or oracle exists anywhere in the tree; every transformation-law statement ultimately rests on Polars 0.46 behavior observed through one golden test and small targeted tests. [source fact — absence]
2. NaN payload and signed-zero survival across `ffi.rs` (both directions) and across Parquet publish/read is covered by no test. [requires experiment]
3. Timezone retention evidence is limited to `"UTC"` (`t48`); other zone strings, fixed offsets, and DST-bearing zones have no coverage. [requires experiment]
4. Trim's exact accepted-whitespace codepoint set is unpinned; no fixture enumerates codepoints. [requires experiment]
5. Null-comparison truth tables (`null == x`, `null < x`, compound And/Or with nulls) are not asserted by any test found. [requires experiment]
6. Deadline/cancel overshoot during a blocking Polars `collect()` is acknowledged only in a comment (`preview.rs:437-442`); no measurement exists. [requires experiment]
7. Memory attribution under genuinely concurrent runs is untested: engine tests serialize via `exclusive_test_lock` while `memory.rs` counters are process-global statics. [requires experiment]
8. The materialize path records no plan fingerprint in any persisted artifact; preview-only consumption means plan-digest behavior is unverifiable for snapshots today. [source fact — gap]
9. Validate/Deduplicate/Join/Union semantics exist only as contract prose plus typed-rejection tests; there is no executable reference for their future behavior. [source fact — absence]

---

## 7. Candidate seam inventory

Non-binding inventory of existing private boundaries that physically separate concerns today. Nothing here proposes a design; each entry cites current symbols and their owner. All items are [source fact] for existence; the "could host" phrasing is [XR-C0 decision input] framing only.

| # | Seam (symbol) | Owner crate/module | Why it is a seam today |
| --- | --- | --- | --- |
| 1 | `preflight.rs:CompiledStep{Project,Filter,Rules}` + `PreparedPlan` | stillflow-engine (private) | An executor-agnostic step list produced before any Polars contact; lowering merely interprets it. Schema propagation already runs over steps without Polars (`preflight.rs:propagate_schema`) |
| 2 | `lower.rs:transform(frame, schema, steps)` | stillflow-engine (private) | Single choke point where steps meet a concrete frame; the entire rule/expr execution vocabulary of the engine passes through this one function |
| 3 | `ffi.rs:{record_batch_to_dataframe, dataframe_to_record_batch}` + static layout asserts (`29-32`) | stillflow-engine (private) | The complete Arrow↔Polars surface of the engine; isolated, counted (`IMPORT_COUNT`, test-only), and failure-releasing (`t36`, `t39`) |
| 4 | `types.rs:polars_data_type` and `types.rs:fixed_slot_bytes` | stillflow-engine (private) | The only LogicalType→physical mapping tables in the engine; one returns Polars dtypes, the other executor-neutral slot sizes used by prediction |
| 5 | `typing.rs:type_check_expr/infer_type/require_boolean/reject_paused_expr` | stillflow-engine (private) | Expression typing fully separated from lowering; contains no Polars types |
| 6 | `remainder.rs:CanonicalRebatcher` + `ColumnSink`/`ExactPrimitiveSink`/`VariableBytes`/`BitPackedSink` family | stillflow-engine (private) | Canonical output construction built purely on arrow-rs builders with exact byte prediction — already independent of any cleaning executor |
| 7 | `predict.rs:PredictedSchema/predict/largest_feasible_k` | stillflow-engine (private) | Physical size model parameterized by logical types + observed Arrow arrays, not by Polars internals |
| 8 | `memory.rs:AllocatorPhase{Idle,Polars,Remainder,StorageAppend}` + `MemoryTracker` | stillflow-engine (private) | Accounting already distinguishes executor phases; `hold_polars` is the only Polars-named accounting hook |
| 9 | `capabilities.rs:Capability::ColumnProjection` gate (`Capability::Streaming` likewise) | stillflow-connectors | Pushdown decisions are expressed as registry capabilities queried in preflight (`preflight.rs:126-132`), not hardcoded to one adapter |
| 10 | `connector-local-tabular/src/bridge/mod.rs:dataframe_to_record_batch` (+ `bridge/ffi.rs`) | stillflow-connector-local-tabular (private) | The connector's own Polars→Arrow seam; decoder swap would be contained because all decode modules are private and the trait boundary is `SourceConnector` |
| 11 | `domain/read.rs:ReadRequest{projection, filter}` | stillflow-core | Declared pushdown channels in the public read contract; `projection` is used conditionally, `filter` currently always `None` from the engine |
| 12 | `store.rs:{SnapshotStore.begin_snapshot, SnapshotWriter.append/commit}` | stillflow-storage | Publication API the engine consumes without knowing Parquet/SQLite details; already executor-agnostic |
| 13 | `core/src/stream/mod.rs:attach_request_context` / `RawBatchStream` newtype | stillflow-core / stillflow-connectors | Stream wrapper layer where cancellation/deadline/lineage enforcement is composed independently of adapters |

Explicitly out of scope of this section (per assignment): no traits, no PhysicalPlan schema, no ADR language. The table records where seams already exist, nothing more.

---

## Issue #94 acceptance checklist

Note on provenance: the Issue #94 text itself is not present in this tree (`docs/issues/` contains issues up to #054 plus inventory documents; no issue-094 file). The checklist below is reconstructed from the canonical deliverable requirements given for XR-D0. Each box is left unchecked with an honest status note. [source fact — absence of issue-094 file; checklist reconstruction per assignment]

- [ ] Header records base SHA `c0e828031f0141fa89e6b525b4314ebabd5f4f4e`, target branch, date 2026-08-23, and scope statement.
  Status: done in header above; SHA verified against `git rev-parse` in the worktree.
- [ ] Section 1 maps every public Engine/Core request/result/type involving LogicalPlan, BatchEnvelope, Snapshot, Verification, and execution identities.
  Status: done; engine, core, plan, connectors, storage, api surfaces enumerated; api crate is a stub (only `crate_name`).
- [ ] Section 1 proves whether Polars/DuckDB/raw-Arrow physical types escape into stable public APIs.
  Status: done — no Polars escape found; no DuckDB dependency exists; raw `RecordBatch`/`SchemaRef` publicly reachable via `stillflow-core::batch` accessors (interpretation deferred to XR-C0).
- [ ] Section 2 enumerates every Polars import and call site, both conversion directions, stage ownership, and Cargo-level dependency evidence.
  Status: done — 15 files greppable, 7 non-test hit files itemized symbol-by-symbol; both conversion funnels identified; per-crate manifest lines quoted; two absences (no verify/recover callers; no plan fingerprint on materialize path) stated explicitly.
- [ ] Section 3 provides the full nine-operator ownership matrix with per-cell evidence and unknown cells marked.
  Status: done; Validate/Deduplicate/Join/Union cells marked rejected-with-evidence; scan pushdown future marked unknown.
- [ ] Section 4 classifies semantic claims using only the five allowed labels.
  Status: done — SF/P/A/S/U applied per row; no performance/equivalence/portability claims made.
- [ ] Section 5 maps runtime-authority responsibilities to symbols and states whether each sits above the executor boundary.
  Status: done; cancellation/deadline granularity limits labeled as static inference requiring experiment, not fact.
- [ ] Section 6 distinguishes contract-law tests from Polars-specific regressions and lists factual gaps without proposing designs.
  Status: done; classification labeled as static inference since the test file carries no such labels; nine gaps listed.
- [ ] Section 7 inventories existing seams non-bindingly, citing symbols/owners, drafting nothing.
  Status: done; 13 seams, existence cited as source fact, forward-looking framing quarantined as decision-input notes.
- [ ] Every material statement carries exactly one classification label inline, and every `[source fact]` cites `path:symbol`.
  Status: done throughout; config/manifest facts cite `path` per the assignment's citation rule.
- [ ] Document makes no performance, equivalence, or portability claims without executable evidence, and never implies Arrow is an executor or AI a deterministic backend.
  Status: conformed; the only AI mention is the accepted-contract quote context absent here by design — no such claim appears.
- [ ] Document is the sole file output of this task, written read-only against sources.
  Status: yes — single markdown file at `.acceptance-tmp/xr-d0/inventory.md`; no repository file was modified.

*End of XR-D0 inventory.*
