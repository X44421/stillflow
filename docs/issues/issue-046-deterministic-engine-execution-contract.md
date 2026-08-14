# Issue #46 Implementation Contract: deterministic single-source execution

> Status: Frozen
> Risk: High
> Issue: #46
> Parent: #3
> Authorized base: `main@1021103238bba89b4a457891eb4484582f5077a9`
> Last updated: 2026-08-14

This document freezes the physical execution boundary. It does not authorize
runtime code. Implementation of the Polars executor (Engine E2) must not start
until architecture review approves this contract.

## 1. Objective

Freeze the contract from a validated `LogicalPlan` to a bounded `BatchStream`
and an atomically published Snapshot. The first executable path is a
deterministic, single-source, linear unary pipeline.

Polars 0.46 is the sole canonical cleaner. Apache Arrow 59 remains the
interchange protocol. `stillflow-storage` remains the only publisher of
visible snapshots. The engine must not depend on a concrete connector adapter
crate.

## 2. Source policy

- The authorized `main` SHA above is the only research and documentation base
  for this PR.
- After approval, E2 must rebuild from the latest accepted `main` at that time.
- Historical branches are read-only. They must not be merged, rebased, or
  cherry-picked.
- Dependabot branches are read-only. Engine branches must not mix Dependabot
  version or lockfile updates.
- This PR is docs-only. It must not modify Rust sources, `Cargo.toml`,
  `Cargo.lock`, frontend files, or CI workflows.

## 3. Risk and compatibility

This work is `risk:high` because it defines the public engine execution API,
operator coverage, type-error semantics, FFI ownership, resource ceilings, and
snapshot publication rules that later Engine deliveries must obey.

Compatibility decision:

- No compatibility shim is provided for executing `Join`, `Union`,
  `Rule::Validate`, `Rule::Deduplicate`, DuckDB SQL, SQLx, or arbitrary engine
  code.
- `LogicalPlan`, `Rule`, `Expr`, `BatchEnvelope`, and `SnapshotDraft` version 1
  contracts remain unchanged.
- E2 may add public types to `stillflow-engine` only as named in section 6.
- E2 may add dependencies to `stillflow-engine` only as named in section 5.3.
- No existing public type in `stillflow-core`, `stillflow-plan`,
  `stillflow-connectors`, or `stillflow-storage` may change in E2 unless a new
  frozen contract authorizes it.

## 4. In scope for this contract

Documentation of:

- `Scan.source_asset_id` binding to caller-injected `SourceConnection` and
  `SourceAsset`.
- Phase-1 plan shape: one `Scan`, one `Materialize` root, linear unary path.
- Supported nodes: `Scan`, `Project`, `Filter`, `ApplyRules`, `Materialize`.
- Typed unsupported errors for `Join` and `Union`.
- First rule batch: Rename, Cast, Trim, ReplaceLiteral, FillNull, DropColumn,
  DeriveColumn, FilterRows.
- Deferral of `Validate`, `Deduplicate`, and Rejected Rows to Engine E4.
- Every `Expr` to Polars mapping and type-error semantic.
- ColumnId, display-name, `LogicalSchema`, and Arrow schema propagation.
- Polars 0.46 ↔ Arrow 59 FFI ownership.
- Determinism, memory, cancellation, identity injection, and sanitized errors.
- Public engine API, dependency arrows, and stop conditions.
- Objective acceptance tests that E2 must automate.

## 5. Explicit non-goals

This contract and the E2 implementation it authorizes must not include:

- API routes, Axum handlers, job tables, or HTTP status machines (E5).
- Frontend layout, components, CSS, tokens, or generated types.
- DuckDB, SQLx, ConnectorX, or SQL Connector #9.
- `Join` / `Union` execution.
- `Rule::Deduplicate`, `Rule::Validate`, or Rejected Rows datasets (E4).
- AI execution, Python, SQL strings, or arbitrary Polars/Python/SQL programs.
- Changes to connector adapter crates except through the existing
  `SourceConnector` / `ConnectorRegistry` boundary.
- Moving or deleting the local-tabular scan-time FFI module.
- Dependabot updates mixed into the Engine branch.
- Historical branch merge, rebase, or cherry-pick.

Node-level Preview (E3) reuses this lowering. E2 must not add a second preview
executor. E2 may expose only the materialize path plus a pure preflight helper.

## 6. Dependency direction and FFI ownership

### 6.1 Crate arrows

Architecture review must confirm:

```text
stillflow-api -> stillflow-engine
stillflow-engine -> stillflow-plan, stillflow-connectors, stillflow-storage
stillflow-plan -> stillflow-core
stillflow-connectors -> stillflow-core
stillflow-storage -> stillflow-core
stillflow-core -> no workspace crate
```

`stillflow-engine` currently depends on `stillflow-connectors` and
`stillflow-core` only. E2 is authorized to add `stillflow-plan` and
`stillflow-storage`. It is not authorized to depend on:

- `stillflow-connector-local-tabular`
- `stillflow-connector-workbook`
- `stillflow-connector-object-store`
- `stillflow-api`

Connector adapters remain registered into `ConnectorRegistry` by a caller
above the engine (API/bootstrap). The engine selects an adapter only by
`SourceConnection.kind()` through the registry.

### 6.2 FFI ownership

Two Polars 0.46 ↔ Arrow 59 C Data Interface bridges may exist. They have
different owners:

| Owner | When | Visible to |
| --- | --- | --- |
| `stillflow-connector-local-tabular` scan-time bridge | Connector decode | That adapter crate only |
| `stillflow-engine` execution-time bridge | Arrow envelope ↔ Polars chunk | Engine crate only |

The engine execution-time bridge:

- is the only engine module permitted to reinterpret Arrow C ABI structs;
- must compile-time assert size and alignment against arrow-rs FFI types;
- must move ownership exactly once into arrow-rs, whose `Drop` invokes the
  Polars release callback;
- must apply the same Null-type buffer normalization required by Polars 0.46
  exporting a placeholder buffer while Arrow 59 requires zero buffers;
- must not import connector adapter modules, types, or functions.

`LogicalSchema` ↔ Arrow 59 mapping remains in `stillflow-core`
(`logical_schema_to_arrow` and envelope validation). The engine must not
invent a second logical-to-Arrow table.

`LogicalType` ↔ Polars `DataType` mapping used during lowering is owned by
`stillflow-engine`. It is the canonical **execution** mapping. Connector
scan-time mapping must remain compatible for types those adapters emit, but
the engine must not call those private functions.

### 6.3 Authorized E2 dependencies

E2 may add to `stillflow-engine` only:

- workspace crates: `stillflow-plan`, `stillflow-storage`;
- already-approved workspace third-party crates as needed for async streaming
  (`tokio`, `tokio-util`, `futures`, `thiserror`, `uuid`, `chrono`,
  `arrow-array`, `arrow-schema`);
- `polars` version `0.46` with `default-features = false` and the minimum
  lazy/expression features required by section 11;
- `polars-arrow` version `0.46` for the FFI bridge.

E2 must not add DuckDB, SQLx, Axum, the `arrow` meta crate, or unrelated
version bumps. Lockfile changes are limited to the newly declared engine
packages.

## 7. Public engine API

Names may be organized into modules. Semantics must match this section.
E2 implements these types in `stillflow-engine`. This docs PR must not add
them.

```rust
pub const ENGINE_CONTRACT_VERSION: u16 = 1;
pub const MAX_PLAN_NODES: usize = 64;
pub const MAX_RULES_PER_NODE: usize = 256;
pub const MAX_EXPR_NODES: usize = 1_024;
pub const MAX_EXPR_DEPTH: usize = 64;
pub const MAX_OPERATOR_STATE_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_ENGINE_CONCURRENT_RUNS: u16 = 4;
pub const ENGINE_DEFAULT_DEADLINE: Duration = Duration::from_secs(15 * 60);
pub const ENGINE_MAX_DEADLINE: Duration = Duration::from_secs(30 * 60);

pub struct ExecutionIdentities {
    pub snapshot_id: Uuid,
    pub dataset_id: Uuid,
    pub session_id: Uuid,
    pub created_at: DateTime<Utc>,
}

pub struct ExecutionRequest<'a> {
    pub plan: LogicalPlan,
    pub connection: SourceConnection,
    pub asset: SourceAsset,
    pub schema_override: Option<LogicalSchema>,
    pub identities: ExecutionIdentities,
    pub context: RequestContext,
    pub batch_size: usize,
    pub store: &'a SnapshotStore,
}

pub struct ExecutionEngine { /* registry only */ }

impl ExecutionEngine {
    pub fn new(registry: ConnectorRegistry) -> Self;
    pub fn preflight(
        &self,
        plan: &LogicalPlan,
        connection: &SourceConnection,
        asset: &SourceAsset,
        schema_override: Option<&LogicalSchema>,
    ) -> Result<PreparedPlan, EngineError>;
    pub async fn materialize(
        &self,
        request: ExecutionRequest<'_>,
    ) -> Result<SnapshotManifest, EngineError>;
}
```

`PreparedPlan` is a validated, schema-propagated, engine-owned compilation of
the linear plan. It must not contain Polars `DataFrame` values, DuckDB
connections, credentials, or source cell values. E3 preview must reuse
`preflight` and the same operator lowering as `materialize`.

`ExecutionEngine` must be `Send + Sync`. Concurrent `materialize` calls on one
instance are capped by `MAX_ENGINE_CONCURRENT_RUNS` and also by
`stillflow-storage::MAX_ACTIVE_PUBLISHERS` (8). Exceeding the engine cap
returns a typed busy error without opening a connector stream.

## 8. Scan binding

`PlanNodeKind::Scan.source_asset_id` is a logical identity. It does not embed a
path, SQL, or credential.

Binding rules, all checked in `preflight` before any batch is read:

1. The plan contains exactly one `Scan` node.
2. `Scan.source_asset_id` equals the injected `SourceAsset.id`.
3. `SourceAsset.id` is not nil.
4. `SourceAsset.connection_id` equals the injected `SourceConnection.id()`.
5. `SourceConnection` validates under the existing domain constructor rules.
6. The registry has an adapter for `SourceConnection.kind()`.
7. That adapter advertises `Capability::Streaming`.
8. `ConnectorKind::SqlDatabase` and `ConnectorKind::DocumentWorker` return
   `UnsupportedCapability` in this phase. Local file, workbook, and object
   store kinds are eligible when registered.

The engine must not look up connections or assets from SQLite, the filesystem,
or environment variables. The caller injects both objects. The engine must not
resolve `CredentialRef` values; connectors continue to do that internally.

`ReadRequest` construction:

- `asset` is the injected `SourceAsset`;
- `schema_override` is the authorized Scan input schema from section 12.2;
- `projection` is `Scan.projection` when the adapter advertises
  `Capability::ColumnProjection`, otherwise `None` and Engine applies
  `Project` after the stream;
- `filter` is always `None` in E2 (no predicate pushdown);
- `checkpoint` is always `None`;
- `batch_size` is the request batch size;
- `context` is the request `RequestContext`.

`Scan.predicate`, when present, is compiled as an in-engine `Filter` immediately
after the scan projection. It uses the same semantics as `PlanNodeKind::Filter`.

Mismatch of `source_asset_id`, nil identities, or connection/asset disagreement
is `EngineError::SourceBinding` with `ErrorCategory::InvalidConfiguration` or
`NotFound` as specified in section 16. The engine never starts a stream on a
binding failure.

## 9. Phase-1 plan shape

`LogicalPlan::validate()` remains necessary and insufficient. Engine preflight
adds:

| Check | Required shape |
| --- | --- |
| Node count | `1..=MAX_PLAN_NODES` |
| Scan count | exactly 1 |
| Materialize count | exactly 1 |
| Root | the `Materialize` node |
| Inputs | every non-Scan node has exactly one input |
| Connectivity | every node lies on the unique path from Scan to Materialize |
| `Join` / `Union` | typed `UnsupportedOperator`; no stream |
| `Rule::Validate` / `Rule::Deduplicate` | typed `UnsupportedRule`; no stream |
| Empty / extra disconnected nodes | typed `InvalidPlan` |

Supported operator order is any linear sequence of unary nodes:

```text
Scan -> (Project | Filter | ApplyRules)* -> Materialize
```

`ApplyRules.rules.len()` must be `1..=MAX_RULES_PER_NODE`. Each rule is applied
in listed order. Multiple `ApplyRules` nodes are allowed.

## 10. Operator semantics

Operators are stateless with respect to row identity. They must not sort,
shuffle, sample, or hash-aggregate. Row order of the Scan stream is the row
order of the Snapshot.

### 10.1 Scan

Emits envelopes for the bound asset. Output schema is the Scan projection
applied to the authorized input schema. Sequence starts at 0 on the connector
stream. Engine output sequences are assigned after canonical rebatching and
also start at 0.

### 10.2 Project

Reorders and subsets columns by `ColumnId`. Unknown IDs are `UnknownColumn`.
Duplicates are already rejected by plan validation. Schema field order becomes
the projection order. ColumnIds and names are otherwise unchanged.

### 10.3 Filter

Keeps a row only when the predicate evaluates to Boolean `true`. `false` and
null drop the row. Schema is unchanged.

### 10.4 ApplyRules

Applies each supported rule in order, updating the working schema after every
rule that changes it. See section 11.

### 10.5 Materialize

Identity transform on rows and schema. `output_label` is a non-empty logical
label already validated by `stillflow-plan`. It is not a filesystem path and
must not be parsed as one. The engine may copy it into snapshot lineage
metadata only if that metadata field already exists; E2 must not add a new
storage column for it. The label is used for error context (`output_label`
string only) and caller correlation.

### 10.6 Join and Union

`preflight` returns `EngineError::UnsupportedOperator` with the node id and
kind name (`join` or `union`). Category: `UnsupportedCapability`. Retryable:
false. No connector I/O, no snapshot writer.

## 11. First-batch rules and Expr lowering

### 11.1 Polars column identity

Polars addresses columns by name. The engine maintains an ordered working
`LogicalSchema` whose field order is the Polars column order. For each
`ColumnId`, the current `LogicalField.name` is the Polars column name.

Rename changes `name` and never `id`. Derive introduces a caller-supplied
`ColumnId` already present in the plan. The engine must not generate column
IDs, plan node IDs, snapshot IDs, dataset IDs, session IDs, or timestamps.

### 11.2 Rule table

| Rule | Schema effect | Runtime |
| --- | --- | --- |
| `Rename { column, to }` | same id; name becomes `to`; names remain unique | Polars `rename` |
| `Cast { column, data_type, on_failure }` | field type becomes `data_type`; `SetNull` forces `nullable = true`; `Error` keeps nullability unless the target type requires it | strict cast; see 11.4 |
| `Trim { column }` | unchanged; column must be `Utf8` | Unicode whitespace trim; null stays null |
| `ReplaceLiteral { column, from, to }` | unchanged | exact replacement; `from`/`to` types must be compatible with the column; `from = Null` replaces nulls |
| `FillNull { column, value }` | field becomes non-nullable when `value` is non-null | fill nulls only |
| `DropColumn { column }` | remove the field; remaining order preserved; at least one field must remain | drop named column |
| `DeriveColumn { id, name, data_type, nullable, expression }` | append one field; `id` and `name` unique | evaluate `expression`, cast to `data_type` |
| `FilterRows { predicate }` | unchanged | same as `Filter` |

`Rule::Validate` and `Rule::Deduplicate` are rejected in preflight. Rejected
Rows datasets are not created.

Trim uses Polars 0.46 default Unicode whitespace stripping (`str.strip_chars`
with no extra character set). It must not alter interior whitespace.

ReplaceLiteral is exact scalar equality, not regex, not collation-insensitive
matching. Float literals remain the canonical finite values already required
by `ScalarValue`.

Dropping the last remaining column is `InvalidPlan` / `EmptyCollection`.

DeriveColumn evaluates `expression` against the pre-derive schema, then casts
to `data_type` with `CastFailurePolicy::Error`. The declared `nullable` flag
must be `true` if the expression can be null; a false flag with a nullable
expression is a preflight type error.

### 11.3 Expr → Polars mapping

Every `Expr` node maps as follows. Preflight type-checks against the working
schema. Runtime uses the compiled Polars expression. No `map`/`apply` Rust
closures over cell values are authorized.

| `Expr` | Polars | Result type |
| --- | --- | --- |
| `Column(id)` | `col(current_name(id))` | field type |
| `Literal(Null)` | typed null literal | `Null` |
| `Literal(Boolean)` | boolean lit | `Boolean` |
| `Literal(Int64)` | i64 lit | `Int64` |
| `Literal(UInt64)` | u64 lit | `UInt64` |
| `Literal(Float64)` | finite f64 lit | `Float64` |
| `Literal(Utf8)` | utf8 lit | `Utf8` |
| `Unary { Not, e }` | `e` Boolean; `not` | `Boolean` |
| `Unary { Negate, e }` | numeric `e`; unary minus | same numeric type |
| `Binary { Equal, NotEqual }` | comparable pair; `eq` / `neq` | `Boolean` |
| `Binary { Lt, Le, Gt, Ge }` | ordered pair | `Boolean` |
| `Binary { And, Or }` | both Boolean | `Boolean` |
| `Binary { Add, Subtract, Multiply }` | numeric pair; result is version-1 LUB | LUB type |
| `Binary { Divide, Modulo }` | numeric pair; zero divisor is `InvalidData` | LUB type |
| `Binary { Contains }` | both `Utf8`; case-sensitive substring | `Boolean` |
| `IsNull { e, negated }` | `is_null` / `is_not_null` | `Boolean` |
| `Cast { e, data_type }` | explicit cast with `Error` policy | `data_type` |
| `Coalesce { exprs }` | first non-null; types combined by LUB | LUB type |

Comparable pairs are those for which version-1 `least_upper_bound` succeeds,
plus Boolean/Utf8/Binary/Date32/Timestamp equality with identical types.
Ordered pairs are signed integers, unsigned integers, floats, `Date32`, and
timestamps with equal timezone. Mixing incomparable types is a preflight
`TypeError`.

`And` / `Or` use three-valued Boolean logic already implied by Polars nulls.
`Filter` / `FilterRows` still keep only `true`.

Expression resource limits, counted iteratively:

- node count `<= MAX_EXPR_NODES`;
- nesting depth `<= MAX_EXPR_DEPTH`;
- every referenced `ColumnId` exists in the working schema.

Recursive type-checking proportional to untrusted depth is forbidden.

### 11.4 Cast and arithmetic errors

`CastFailurePolicy::Error` and DeriveColumn casts: the first unrepresentable
value fails the run with `EngineError::CastFailure`. The error may include
column id, logical type names, batch sequence, and row offset inside the
batch. It must not include the cell value.

`CastFailurePolicy::SetNull`: unrepresentable values become null; the field
is nullable. The run continues.

Integer or float division/modulo by zero, and any arithmetic that would
produce a non-finite float, are `EngineError::Arithmetic`. Same sanitization
rules. Silent `inf` / `NaN` introduction is forbidden.

Overflow of integer arithmetic is `EngineError::Arithmetic`.

## 12. Schema and Arrow propagation

### 12.1 Authorized Scan input schema

If `schema_override` is present, it is the authorized Scan input schema after
`LogicalSchema::validate()`. If absent, `preflight` may call
`ConnectorRegistry::inspect` once and use `AssetMetadata.schema`.

The authorized schema must contain every `Scan.projection` id. Unknown
projection ids are `UnknownColumn`.

### 12.2 Working schema

Each operator produces a new working `LogicalSchema` version 1:

- field order is meaningful;
- `ColumnId` values stay unique;
- display names stay unique and non-empty;
- metadata remains secret-free;
- schema resource ceilings from #30 remain in force
  (`MAX_SCHEMA_FIELDS`, `MAX_SCHEMA_NESTING_DEPTH`, `MAX_SCHEMA_TEXT_BYTES`).

The Materialize working schema is the Snapshot schema. Engine constructs one
`BatchEnvelopeFactory` from that schema and the bound `source_asset_id` and
reuses it for every output envelope.

### 12.3 Stream schema constancy

Every connector envelope must match the authorized projected Scan schema
(logical equality and fingerprint). Drift is `ErrorCategory::SchemaDrift`.
The engine must not widen, rename, or coerce connector fields to keep the
stream alive.

### 12.4 Arrow 59

Output envelopes use `stillflow-core` canonical Arrow metadata:

- `stillflow.schema.version`
- `stillflow.schema.fingerprint`
- `stillflow.schema.metadata`
- `stillflow.column.id`
- `stillflow.field.metadata`

Engine must not write a second metadata vocabulary. Polars names are an
execution convenience and must be reconstructed from `LogicalSchema` before
`BatchEnvelopeFactory::try_from_batch`.

## 13. Determinism

A run is deterministic when all of the following hold.

1. Identical authorized input rows, identical validated plan, identical
   `batch_size`, and identical injected identities produce identical ordered
   logical output rows and an identical output `LogicalSchema`.
2. Changing only the connector's input batch partitioning must not change
   those logical rows, the schema, or `SnapshotStats.row_count`. With a fixed
   `batch_size`, canonical rebatching also keeps output envelope boundaries
   and therefore `partition_count` unchanged.
3. The engine must not read random number generators, system clocks, locale,
   process id, or unordered `HashMap` iteration to decide row values, column
   order, envelope sequence, or fingerprints.
4. Injected `created_at` is the only timestamp written into `SnapshotDraft`.
   `RequestContext` deadlines may observe `Instant` solely to abort.
5. Canonical bytes of the plan remain the `stillflow-plan` fingerprint. The
   engine must not re-canonicalize with a different algorithm.

Non-determinism in a source file (for example concurrent mutation) is a
connector/`InvalidData` or `SchemaDrift` failure, not an engine license to
reorder rows.

## 14. Resource bounds

Every limit below is a hard ceiling. Exceeding it fails the run with a typed
error. No unbounded collect, prefetch queue, or full-source materialization
is authorized.

| Resource | Ceiling | Source |
| --- | --- | --- |
| Input envelope rows | `MAX_BATCH_ROWS` = 65,536 | `stillflow-core` |
| Input envelope Arrow bytes | `MAX_BATCH_BYTES` = 64 MiB | `stillflow-core` |
| Output envelope rows | `MAX_BATCH_ROWS` | same |
| Output envelope Arrow bytes | `MAX_BATCH_BYTES` | same |
| Request `batch_size` | `1..=ReadRequest::MAX_BATCH_SIZE` (65,536) | `stillflow-core` |
| Canonical output pack size | `batch_size` rows, then byte cap | this contract |
| Operator extra state | `MAX_OPERATOR_STATE_BYTES` = 32 MiB | this contract |
| Plan nodes | `MAX_PLAN_NODES` = 64 | this contract |
| Rules per `ApplyRules` | `MAX_RULES_PER_NODE` = 256 | this contract |
| Expr nodes | `MAX_EXPR_NODES` = 1,024 | this contract |
| Expr depth | `MAX_EXPR_DEPTH` = 64 | this contract |
| Schema fields / depth / text | 4,096 / 64 / 1 MiB | #30 |
| Snapshot input envelopes | `MAX_INPUT_ENVELOPES` = 16,384 | `stillflow-storage` |
| Snapshot partitions | `MAX_SNAPSHOT_PARTITIONS` = 16,384 | `stillflow-storage` |
| Snapshot rows | `MAX_SNAPSHOT_ROWS` = 1,000,000,000 | `stillflow-storage` |
| Snapshot stored bytes | `MAX_SNAPSHOT_STORED_BYTES` = 1 TiB | `stillflow-storage` |
| Engine concurrent runs | `MAX_ENGINE_CONCURRENT_RUNS` = 4 | this contract |
| Storage publishers | `MAX_ACTIVE_PUBLISHERS` = 8 | `stillflow-storage` |
| Default deadline | `ENGINE_DEFAULT_DEADLINE` = 15 min | this contract |
| Maximum deadline | `ENGINE_MAX_DEADLINE` = 30 min | this contract |
| E3 preview rows / bytes | 10,000 / 50 MiB | `PreviewRequest` |
| E3 preview deadline | 30 s | E3; recorded here |

Memory law for one `materialize` call:

```text
O(input_batch + canonical_output_batch + bounded_operator_state)
```

`bounded_operator_state` includes the compiled expression tree, working
schema, ColumnId maps, FFI scratch, and at most one Polars chunk aligned to
the current input envelope. It excludes the durable snapshot partitions,
which remain the storage crate's concern and are still subject to the
snapshot byte ceiling.

Canonical rebatching may hold a remainder smaller than `batch_size`. That
remainder counts as the current output batch, not as extra operator state.

If `RequestContext` has no deadline, the engine applies
`ENGINE_DEFAULT_DEADLINE` from the start of `materialize`. A caller-supplied
deadline longer than `ENGINE_MAX_DEADLINE` is rejected in preflight.

## 15. Cancellation, failure, and snapshots

Cancellation and deadline must be observed:

1. before `preflight` inspect I/O;
2. before opening `ConnectorRegistry::read_batches`;
3. on every connector stream poll (existing `attach_request_context`);
4. before lowering each input envelope;
5. before `SnapshotWriter::append`;
6. before `SnapshotWriter::commit`.

A cancelled or timed-out run returns `ErrorCategory::Cancelled` or
`Timeout`. It must not return a `SnapshotManifest`.

`SnapshotDraft` identities are caller-injected:

- `snapshot_id`
- `dataset_id`
- `session_id`
- `created_at`
- `source_asset_id` from the bound asset

The engine must not call `Uuid::new_v4()` or `Utc::now()` for those fields.
Nil identities are rejected before a writer is created.

Failure or cancellation after `begin_snapshot` must drop `SnapshotWriter`
without `commit`. Storage abort semantics already remove staging and
unpublished partitions. Tests must assert that `load_manifest(snapshot_id)`
fails and that no visible snapshot exists.

A successful `commit` is the only publication point. Readers must never
observe a partial snapshot. Engine must not write partitions except through
`SnapshotWriter::append`.

Empty successful runs (zero rows) are valid. They produce a snapshot with
zero partitions and zero stored bytes, matching `SnapshotStats` conservation.

## 16. Errors and security

All engine failures are typed. Production paths return `EngineError`; they do
not panic, `unwrap`, or `expect`.

`EngineError` must map to `ErrorCategory` and a sanitized user message.
Allowed message contents: node id, column id, field name, operator/rule kind,
logical type names, counts, limits, batch sequence, row offset, output label.

Forbidden in errors, events, logs, and metadata:

- raw cell values;
- credentials or `CredentialRef` internals beyond the already-safe `cred://`
  display form already accepted by core;
- full filesystem paths, object URLs, SQL, or connection config dumps;
- Polars/DuckDB engine backtraces that embed values.

Suggested variants (names may differ; semantics must exist and be tested):

- `UnsupportedOperator`
- `UnsupportedRule`
- `UnsupportedCapability`
- `SourceBinding`
- `InvalidPlan`
- `UnknownColumn`
- `TypeError`
- `CastFailure`
- `Arithmetic`
- `SchemaDrift`
- `BoundExceeded` (rows, bytes, nodes, time, concurrency)
- `Cancelled`
- `Timeout`
- `Busy`
- `Storage` (wrapper around `StorageError` without leaking paths)
- `Connector` (wrapper around `ConnectorError`)

`EngineError` must not implement `Display` by forwarding unsanitized
third-party strings. Connector and storage wrappers reuse those crates'
already-sanitized user messages.

## 17. E2 vertical slice

After this contract is approved, E2 rebuilds from the latest `main` and
implements one vertical path only:

```text
Connector BatchStream
  -> LogicalPlan preflight
  -> Stateless Polars lowering
  -> Canonical output batching
  -> BatchEnvelopeFactory
  -> SnapshotWriter
  -> Atomic commit
```

E2 tests must cover:

- batch-size invariance of logical rows, schema, and row_count;
- schema drift abort without a visible snapshot;
- `Join` / `Union` / `Validate` / `Deduplicate` preflight errors;
- cancellation before stream start, during lowering, and before commit;
- deadline abort at the same checkpoints;
- failure without partial snapshot;
- Rust fmt, Clippy, workspace tests;
- frontend typecheck and build (unchanged files, still required).

E2 must not implement the remaining rule kinds, Preview HTTP, or job runtime.

## 18. Implementation checklist

This checklist is for the later E2 PR. This docs PR only records it.

1. Add `stillflow-plan` and `stillflow-storage` to `stillflow-engine`.
2. Add Polars 0.46 / `polars-arrow` 0.46 as specified.
3. Implement private FFI bridge with size/alignment asserts.
4. Implement `EngineError` and sanitization.
5. Implement `preflight` plan-shape, binding, and type-checking.
6. Implement Scan read through `ConnectorRegistry` only.
7. Implement stateless lowering for the five nodes and eight rules.
8. Implement canonical rebatching and one `BatchEnvelopeFactory`.
9. Implement `materialize` with injected identities and abort-on-drop.
10. Add the automated tests listed in section 19.
11. Run repository checks. Do not include Dependabot diffs.

## 19. Acceptance criteria

### 19.1 This docs PR

- Diff contains only Issue #46, this contract, and necessary roadmap updates.
- No Rust runtime file, `Cargo.toml`, `Cargo.lock`, frontend file, or workflow
  file is modified.
- Every memory, row, byte, time, and concurrency limit in section 14 has an
  explicit numeric ceiling.
- Every criterion in section 19.2 is written so E2 can automate it.
- Dependency arrows in section 6.1 match ADR-001 and `AGENTS.md`.
- Work stops after architecture review. E2 does not start in this PR.

### 19.2 E2 automated tests (must be expressible now)

| ID | Criterion | Automated evidence |
| --- | --- | --- |
| T01 | Linear `Scan -> Project -> Filter -> ApplyRules -> Materialize` materializes | fixture snapshot row_count and schema |
| T02 | Two input partitionings of the same rows yield equal logical rows and stats.row_count | compare collected columns |
| T03 | Fixed `batch_size` yields equal output envelope boundaries | compare sequences and per-envelope row_count |
| T04 | `Join` preflight is `UnsupportedOperator`; no snapshot | error category + `load_manifest` fails |
| T05 | `Union` preflight is `UnsupportedOperator`; no snapshot | same |
| T06 | `Rule::Validate` / `Rule::Deduplicate` preflight is `UnsupportedRule` | same |
| T07 | Scan id mismatch is `SourceBinding` before stream | mock registry poll-count = 0 |
| T08 | Connector schema drift aborts and publishes nothing | `SchemaDrift` + no manifest |
| T09 | Cancel before `read_batches` publishes nothing | `Cancelled` + no manifest |
| T10 | Cancel during lowering publishes nothing | same |
| T11 | Cancel after append, before commit, publishes nothing | same |
| T12 | Deadline before commit publishes nothing | `Timeout` + no manifest |
| T13 | Cast `Error` fails without embedding the cell value | message regex forbids digits of the fixture value |
| T14 | Cast `SetNull` writes null and continues | null count |
| T15 | Trim / ReplaceLiteral / FillNull / DropColumn / Rename / DeriveColumn / FilterRows match fixtures | golden logical rows |
| T16 | Unknown `ColumnId` is `UnknownColumn` | typed error |
| T17 | Incomparable `Expr` types are `TypeError` at preflight | no stream |
| T18 | Division by zero is `Arithmetic` without cell values | typed error |
| T19 | Engine crate depends on registry, not adapter crates | `cargo tree -p stillflow-engine -i stillflow-connector-local-tabular` fails |
| T20 | `stillflow-engine` depends on plan, connectors, storage, core | `cargo tree` arrows |
| T21 | Injected snapshot/session/dataset ids and `created_at` appear unchanged | manifest equality |
| T22 | Engine does not call `Uuid::new_v4` / `Utc::now` on the materialize path | source grep of engine runtime excluding tests |
| T23 | Memory law: no full-source `collect` of the connector stream | source grep + streaming fixture larger than `MAX_OPERATOR_STATE_BYTES` |
| T24 | Concurrent fifth run on one engine is `Busy` | semaphore test |
| T25 | Empty source commits a zero-row snapshot | stats conservation |
| T26 | `cargo fmt --all -- --check` | CI |
| T27 | `cargo clippy --workspace --all-targets -- -D warnings` | CI |
| T28 | `cargo test --workspace` | CI |
| T29 | `npm run typecheck` and `npm run build` | CI, unchanged frontend |

## 20. Stop conditions

Stop and return to contract review if implementation needs:

- a public type not named in section 7;
- a dependency not named in section 6.3;
- engine imports of a connector adapter crate;
- a second cleaning semantics in DuckDB or SQL;
- unbounded collect, prefetch, or full-source materialization;
- generated snapshot/session/dataset ids or timestamps;
- Join/Union/Validate/Deduplicate execution;
- a compatibility shim;
- Dependabot or unrelated lockfile edits;
- a message that includes a cell value or credential.

## 21. Known risks

- Two FFI bridges can drift. Engine tests must cover the Null-type buffer
  layout and the supported type matrix independently of the connector crate.
- Polars name-based execution can collide if rename/derive uniqueness checks
  are skipped. Preflight uniqueness is mandatory.
- Canonical rebatching interacts with `MAX_BATCH_BYTES`. A single wide row
  that exceeds the byte cap fails; it must not be silently split by column.
- `MAX_ENGINE_CONCURRENT_RUNS` is lower than storage publisher capacity so
  tests can saturate the engine cap without depending on storage busy errors.
- SQL Connector #9 and DuckDB #10 remain outside this contract. They must not
  be pulled forward to make a type convenient.
