# Issue #46 Implementation Contract: deterministic single-source execution

> Status: Frozen (revision R3, approved)
> Risk: High
> Issue: #46 (contract), #48 (E2 implementation)
> Parent: #3
> Authorized base: `main@4b652047dcc7253c09ec4e375cb6184d509cacdd`
> Approved SHA: `32f1c53d9903f66aeaca1c2676c0b81abfb2a702`
> Last updated: 2026-08-14
> Review: PR #47 merged. R3 nits applied here before E2: one
> `PredictedColumn.nullable` field, and `MAX_LIVE_COLUMNAR_PAYLOADS`.
> E2 also records workspace `arrow-select` / `arrow-cast` in §6.3.

This document freezes the physical execution boundary. Engine E2 (#48)
implements it from merged `main`. The authorized post-approval nits are
`PredictedColumn.nullable`, `MAX_LIVE_COLUMNAR_PAYLOADS`, and the two
Arrow crates already used by connectors.

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
- PR #47 was docs-only. E2 (#48) may change `stillflow-engine` and add
  `ConnectorRegistry::capabilities` as named in sections 6.3, 6.4, and 7.

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
- E2 may add public types to `stillflow-engine` only as named in section 7.
- E2 may add dependencies to `stillflow-engine` only as named in section 6.3.
- E2 may add exactly one public method to `stillflow-connectors`:
  `ConnectorRegistry::capabilities`, as named in section 6.4. No other public
  connector, core, plan, or storage type may change.

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
- Every `Expr` to Polars mapping and type-error semantic, including
  nullability inference and LUB operand conversion.
- ColumnId, display-name, `LogicalSchema`, and Arrow schema propagation,
  including the four schema stages in section 12.
- Bidirectional Polars 0.46 ↔ Arrow 59 FFI ownership.
- Determinism, pre-Polars execution chunking, peak memory, canonical
  rebatching, cancellation, identity injection, and sanitized errors.
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
- Changes to connector **adapter** crates. The registry crate may gain only
  the method in section 6.4.
- Moving or deleting the local-tabular scan-time FFI module.
- Dependabot updates mixed into the Engine branch.
- Historical branch merge, rebase, or cherry-pick.

Node-level Preview (E3) reuses this lowering. E2 must not add a second preview
executor. E2 may expose only `materialize` plus the async `preflight` helper.
`materialize` must never accept a caller-supplied `PreparedPlan` as authority
to skip validation.

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

### 6.2 Bidirectional FFI ownership

Two Polars 0.46 ↔ Arrow 59 C Data Interface bridges may exist. They have
different owners:

| Owner | When | Visible to |
| --- | --- | --- |
| `stillflow-connector-local-tabular` scan-time bridge | Connector decode | That adapter crate only |
| `stillflow-engine` execution-time bridge | Arrow envelope ↔ Polars chunk | Engine crate only |

The engine execution-time bridge is the only engine module permitted to
reinterpret Arrow C ABI structs. It must compile-time assert size and
alignment of Polars and arrow-rs FFI structs. It must not import connector
adapter modules, types, or functions.

`LogicalSchema` ↔ Arrow 59 mapping remains in `stillflow-core`
(`logical_schema_to_arrow` and envelope validation). The engine must not
invent a second logical-to-Arrow table.

`LogicalType` ↔ Polars `DataType` mapping used during lowering is owned by
`stillflow-engine`. It is the canonical **execution** mapping. Connector
scan-time mapping must remain compatible for types those adapters emit, but
the engine must not call those private functions.

#### 6.2.1 Polars → Arrow 59 (export)

- Polars is the producer. Export each array/field through Polars' C ABI
  (`export_array_to_c` / `export_field_to_c`).
- Ownership moves exactly once into arrow-rs via `FFI_ArrowArray::from_raw` /
  `FFI_ArrowSchema::from_raw`. arrow-rs `Drop` invokes the Polars release
  callback.
- After `from_raw`, the Polars exporter wrapper must be inert so it does not
  release a second time.
- Polars 0.46 exports one placeholder buffer for physical `Null`. Arrow 59
  requires zero buffers. The engine may zero `n_buffers` / `buffers` on the
  **consumer view only**. The producer release callback still owns
  `private_data` and must not be skipped.

#### 6.2.2 Arrow 59 → Polars (import)

- arrow-rs is the producer. Export each array/field through arrow-rs FFI, then
  import with Polars' C ABI import (`import_array_from_c` /
  `import_field_from_c` or equivalent 0.46 APIs).
- Buffer lifetime: the Arrow buffers remain valid until Polars' imported array
  is dropped and the producer release callback runs. The engine must not drop
  or reuse the source `RecordBatch` until that import succeeds or the export
  is explicitly released.
- After a successful import, Polars owns the array. The engine must not call
  the Arrow release callback itself.
- Arrow 59 `Null` has zero buffers. If Polars 0.46 import rejects that layout,
  the engine may attach one producer-owned empty placeholder buffer for that
  import only. The matching release callback must free it exactly once.
- Schema/Array pairing: field count, per-field `DataType`, and array length
  must match. A mismatch is `EngineError::Ffi` with category `Internal`. No
  Polars batch is retained.

#### 6.2.3 Partial import failure

If column `i` of `n` fails to import:

1. Successfully imported columns `0..i` are dropped. Their Drop path releases
   producer buffers exactly once.
2. Column `i` and any later columns that were exported but not imported are
   released exactly once through the C ABI release callback.
3. No double-free, no leaked export, and no partially constructed Polars
   `DataFrame` escapes the bridge.
4. The engine returns `EngineError::Ffi` and does not append a snapshot
   envelope.

Round-trip tests must cover `Null`, nested `List`/`Struct`, and a mid-schema
import failure.

### 6.3 Authorized E2 dependencies

E2 may add to `stillflow-engine` only:

- workspace crates: `stillflow-plan`, `stillflow-storage`;
- already-approved workspace third-party crates:
  `tokio` (including `sync` for the per-instance `Semaphore` run gate),
  `tokio-util`, `futures`, `thiserror`, `uuid`, `chrono`;
- Arrow FFI crates matching the connector pair:
  `arrow-array` workspace with feature `ffi`,
  `arrow-schema` workspace with feature `ffi`,
  `arrow-data = "59"`;
- already-approved workspace Arrow crates required to keep remainder and
  export on the canonical Arrow 59 types:
  `arrow-select` (canonical remainder concat),
  `arrow-cast` (Polars Utf8View → canonical Utf8 after C ABI export);
- `polars-arrow = "0.46"`;
- `polars` version `0.46` with `default-features = false` and **exactly**
  these features:

```toml
polars = { version = "0.46", default-features = false, features = [
    "lazy",
    "strings",
    "regex",
    "dtype-u8",
    "dtype-u16",
    "dtype-i8",
    "dtype-i16",
    "dtype-date",
    "dtype-datetime",
    "dtype-struct",
] }
```

Those features are the closed set required for Expr evaluation, UTF-8
`strip_chars` / `contains`, and the version-1 logical type matrix. Polars 0.46
does not expose `dtype-u32`; `UInt32` is available without that feature. The
`regex` feature is required for `Expr::Contains` literal substring matching.
IO features
(`csv`, `json`, `parquet`) are forbidden on the engine crate.

`EngineError` must not depend on `serde` / `serde_json`. The public sanitised
event/API surface is `stillflow_core::SanitizedErrorSummary`, which already
serializes in `stillflow-core`. E2 may add `serde_json` as a **dev-dependency
only** for tests that serialize `sanitized_summary()`. Production
`stillflow-engine` must not add `serde` or `serde_json`.

E2 must not add DuckDB, SQLx, Axum, the `arrow` meta crate, or unrelated
version bumps. Lockfile changes are limited to the newly declared engine
packages.

### 6.4 Authorized registry capability query

`ConnectorRegistry::get` and `require` remain `pub(crate)`. E2 must add this
public additive method and no other registry API:

```rust
impl ConnectorRegistry {
    pub fn capabilities(
        &self,
        kind: ConnectorKind,
    ) -> ConnectorResult<ConnectorCapabilities>;
}
```

Behavior:

- unknown kind → the existing typed invalid-configuration error;
- known kind → `connector.capabilities()` by value, no adapter type leak.

Engine preflight uses this method to require `Capability::Streaming` and to
decide whether `Scan.projection` is pushed. Engine must not downcast adapters
and must not depend on adapter crates to read capabilities.

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
pub const MAX_LIVE_COLUMNAR_PAYLOADS: u8 = 3;
pub const MAX_COMPILED_PLAN_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_FFI_SCRATCH_BYTES: usize = 1024 * 1024;
pub const MAX_OPERATOR_STATE_BYTES: usize =
    MAX_COMPILED_PLAN_BYTES + MAX_FFI_SCRATCH_BYTES; // 5 MiB
pub const MAX_ENGINE_PEAK_BYTES: usize =
    (MAX_LIVE_COLUMNAR_PAYLOADS as usize) * MAX_BATCH_BYTES
        + MAX_OPERATOR_STATE_BYTES; // 197 MiB
pub const MAX_ENGINE_CONCURRENT_RUNS: u16 = 4;
pub const ENGINE_DEFAULT_DEADLINE: Duration = Duration::from_secs(15 * 60);
pub const ENGINE_MAX_DEADLINE: Duration = Duration::from_secs(30 * 60);
pub const MAX_BOOL_UTF8_BYTES: usize = 5;
pub const MAX_INT_UTF8_BYTES: usize = 20;
pub const MAX_FLOAT_UTF8_BYTES: usize = 32;
pub const UTF8_VIEW_SLOT_BYTES: usize = 16;
pub const UTF8_OFFSET_SLOT_BYTES: usize = 4;

pub struct ExecutionIdentities {
    pub snapshot_id: Uuid,
    pub dataset_id: Uuid,
    pub session_id: Uuid,
    pub created_at: DateTime<Utc>,
    pub started_at: DateTime<Utc>,
    pub lineage: BTreeSet<Uuid>,
    pub quality_score: Option<u8>,
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

pub struct ExecutionEngine {
    registry: ConnectorRegistry,
    run_gate: tokio::sync::Semaphore,
}

impl ExecutionEngine {
    pub fn new(registry: ConnectorRegistry) -> Self;

    pub async fn preflight(
        &self,
        plan: &LogicalPlan,
        connection: &SourceConnection,
        asset: &SourceAsset,
        schema_override: Option<&LogicalSchema>,
        context: &RequestContext,
    ) -> Result<PreparedPlan, EngineError>;

    pub async fn materialize(
        &self,
        request: ExecutionRequest<'_>,
    ) -> Result<SnapshotManifest, EngineError>;
}

impl EngineError {
    pub fn category(&self) -> ErrorCategory;
    pub fn retryable(&self) -> bool;
    pub fn sanitized_summary(&self) -> SanitizedErrorSummary;
}
```

`new` constructs a `Semaphore` with `MAX_ENGINE_CONCURRENT_RUNS` permits.
`ExecutionEngine` is registry **plus** that per-instance run gate. A
registry-only engine cannot implement the concurrency contract.

`preflight` is async because schema resolution may call
`ConnectorRegistry::inspect`. It receives `RequestContext` and must call
`context.ensure_active()` before inspect and before returning success.
A dry-run `preflight` does **not** acquire the run gate.

`materialize` must, in this order:

1. Apply the default deadline if absent, then `context.ensure_active()`.
2. `try_acquire` one run-gate permit. Do not await a permit. Failure is
   `EngineError::Busy` with inspect count 0 and connector `read_batches` poll
   count 0.
3. Hold the permit until `commit` returns or the run aborts (including
   cancellation, timeout, and `Drop` of the in-flight future).
4. Call `preflight` internally with the request's plan, connection, asset,
   schema override, and context. It must not accept a `PreparedPlan` argument
   and must not reuse a caller-held `PreparedPlan` to skip validation.
5. Open the connector stream, chunk, transform, rebatch, and publish.

`PreparedPlan` is a validated, schema-propagated, engine-owned compilation of
the linear plan. It must not contain Polars `DataFrame` values, DuckDB
connections, credentials, or source cell values. E3 preview must reuse
`preflight` and the same operator lowering as `materialize`.

`ExecutionEngine` must be `Send + Sync`. Concurrent `materialize` calls on one
instance are capped by `MAX_ENGINE_CONCURRENT_RUNS` and also by
`stillflow-storage::MAX_ACTIVE_PUBLISHERS` (8). Exceeding the engine cap
returns `Busy` without inspect or stream I/O.

`EngineError` is not `Serialize`. Tests and later API/event code serialize
`sanitized_summary()` only.

## 8. Scan binding

`PlanNodeKind::Scan.source_asset_id` is a logical identity. It does not embed a
path, SQL, or credential.

Binding rules, all checked in `preflight` before any batch is read:

1. The plan contains exactly one `Scan` node.
2. `Scan.source_asset_id` equals the injected `SourceAsset.id`.
3. `SourceAsset.id` is not nil.
4. `SourceAsset.connection_id` equals the injected `SourceConnection.id()`.
5. `SourceConnection` validates under the existing domain constructor rules.
6. `registry.capabilities(connection.kind())` succeeds.
7. Those capabilities include `Capability::Streaming`.
8. `ConnectorKind::SqlDatabase` and `ConnectorKind::DocumentWorker` return
   `UnsupportedCapability` in this phase. Local file, workbook, and object
   store kinds are eligible when registered.

The engine must not look up connections or assets from SQLite, the filesystem,
or environment variables. The caller injects both objects. The engine must not
resolve `CredentialRef` values; connectors continue to do that internally.

Let `push_projection` be true iff capabilities include
`Capability::ColumnProjection`.

`ReadRequest` construction:

- `asset` is the injected `SourceAsset`;
- `schema_override` is the **authorized source schema** (section 12.1), not
  the Scan output schema;
- `projection` is `Some(Scan.projection)` iff `push_projection`, otherwise
  `None`;
- `filter` is always `None` in E2 (no predicate pushdown);
- `checkpoint` is always `None`;
- `batch_size` is the request batch size;
- `context` is the request `RequestContext`.

When `push_projection` is false, the engine applies `Scan.projection` as an
in-engine `Project` after the connector stream. When true, that Project is
the identity of the connector output.

`Scan.predicate`, when present, is compiled as an in-engine `Filter`
immediately after the Scan output schema is established. It uses the same
semantics as `PlanNodeKind::Filter`.

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
| `Materialize.output_label` | non-empty after trim **and** secret-safe |

Supported operator order is any linear sequence of unary nodes:

```text
Scan -> (Project | Filter | ApplyRules)* -> Materialize
```

`ApplyRules.rules.len()` must be `1..=MAX_RULES_PER_NODE`. Each rule is applied
in listed order. Multiple `ApplyRules` nodes are allowed.

Secret-safe labels: `ensure_no_secret_fields(Value::String(output_label))`
must succeed. Failure is `InvalidPlan` before inspect or stream I/O.
`output_label` must not appear in `EngineError` Display/Debug payloads,
`sanitized_summary()`, or event metadata. Correlation uses the Materialize
`PlanNodeId` only.

## 10. Operator semantics

Operators are stateless with respect to row identity. They must not sort,
shuffle, sample, or hash-aggregate. Row order of the Scan stream is the row
order of the Snapshot.

### 10.1 Scan

Reads connector envelopes whose schema is the **expected connector schema**
(section 12.2). After optional in-engine projection, the Scan **output
schema** is `Scan.projection` applied to the authorized source schema.
Connector stream sequences start at 0.

The engine must **not** lower a full connector envelope through Polars.
Section 14.2 splits each envelope into execution chunks first. Engine output
sequences are assigned after canonical rebatching and also start at 0.

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

Identity transform on rows and schema. `output_label` is a logical label, not
a filesystem path, and must not be parsed as one. E2 must not add a storage
column for it and must not copy it into error messages (section 9).

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
IDs, plan node IDs, snapshot IDs, dataset IDs, session IDs, timestamps,
lineage ids, or quality scores.

### 11.2 Rule table

| Rule | Schema effect | Runtime |
| --- | --- | --- |
| `Rename { column, to }` | same id; name becomes `to`; names remain unique | Polars `rename` |
| `Cast { column, data_type, on_failure }` | field type becomes `data_type`; `SetNull` forces `nullable = true`; `Error` keeps prior nullability | Date32/Timestamp → Utf8 is preflight `TypeError`; otherwise strict cast; see 11.5 |
| `Trim { column }` | unchanged; column must be `Utf8` | Unicode whitespace trim; null stays null |
| `ReplaceLiteral { column, from, to }` | if `to` is `Null`, field becomes nullable; otherwise unchanged | exact replacement; `from`/`to` must be type-compatible with the column; `from = Null` replaces nulls |
| `FillNull { column, value }` | field becomes non-nullable when `value` is non-null | fill nulls only; `value = Null` is a preflight `TypeError` |
| `DropColumn { column }` | remove the field; remaining order preserved; at least one field must remain | drop named column |
| `DeriveColumn { id, name, data_type, nullable, expression }` | append one field; `id` and `name` unique | evaluate `expression`, cast to `data_type` with `Error` policy |
| `FilterRows { predicate }` | unchanged | same as `Filter` |

`Rule::Validate` and `Rule::Deduplicate` are rejected in preflight. Rejected
Rows datasets are not created.

Trim uses Polars 0.46 default Unicode whitespace stripping (`str.strip_chars`
with no extra character set). It must not alter interior whitespace.

ReplaceLiteral is exact scalar equality, not regex, not collation-insensitive
matching. Float literals remain the canonical finite values already required
by `ScalarValue`. `to = Null` is allowed and **must** mark the field
nullable, including when the column was previously non-nullable.

Dropping the last remaining column is `InvalidPlan` / `EmptyCollection`.

DeriveColumn evaluates `expression` against the pre-derive schema, then casts
to `data_type` with `CastFailurePolicy::Error`. Declared `nullable` must be
`true` whenever section 11.4 infers the expression is nullable. Declared
`nullable = true` is allowed when the expression is non-nullable (nullability
widening). Declared `nullable = false` with an inferred-nullable expression
is a preflight `TypeError`.

### 11.3 Expr → Polars mapping and LUB conversion

Every `Expr` node maps as follows. Preflight type-checks against the working
schema. Runtime uses the compiled Polars expression. No `map`/`apply` Rust
closures over cell values are authorized.

When a binary operator requires a version-1 LUB type `T` and the operand
types are not already `T`, both operands are **first cast to `T`** with
`CastFailurePolicy::Error`, then the operator is applied. Identical operand
types skip that cast.

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
| `Binary { Equal, NotEqual }` | comparable pair, LUB-cast if needed; `eq` / `neq` | `Boolean` |
| `Binary { Lt, Le, Gt, Ge }` | ordered pair, LUB-cast if needed | `Boolean` |
| `Binary { And, Or }` | both Boolean | `Boolean` |
| `Binary { Add, Subtract, Multiply }` | numeric pair, LUB-cast if needed | LUB type |
| `Binary { Divide, Modulo }` | see 11.5 | see 11.5 |
| `Binary { Contains }` | both `Utf8`; case-sensitive substring | `Boolean` |
| `IsNull { e, negated }` | `is_null` / `is_not_null` | `Boolean` |
| `Cast { e, data_type }` | explicit cast with `Error` policy; Date32/Timestamp → Utf8 is preflight `TypeError` | `data_type` |
| `Coalesce { exprs }` | first non-null; each arm LUB-cast to the combined LUB | LUB type |

These six `Literal` arms are the complete `ScalarValue` surface. There is no
`Binary`, `Date32`, or `Timestamp` variant, so those literals cannot appear
in a plan.

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

### 11.4 Expression nullability inference

Inference is structural and does not inspect runtime values.

| Node | Inferred nullable |
| --- | --- |
| `Column(id)` | that field's `nullable` flag |
| `Literal(Null)` | `true` |
| `Literal` non-null | `false` |
| `Unary { Not \| Negate, e }` | same as `e` |
| `Binary` arithmetic, comparisons, `Contains`, `And`, `Or` | `true` iff either operand is nullable |
| `IsNull { .. }` | `false` |
| `Cast { e, .. }` | same as `e` |
| `Coalesce { exprs }` | `true` iff **every** arm is nullable |

Nullability inference never uses cell values. Divide-by-zero and overflow
fail the run (section 11.5); they do not introduce nulls.

### 11.5 Cast, divide, modulo, and overflow

`CastFailurePolicy::Error` and DeriveColumn casts: the first unrepresentable
value fails the run with `EngineError::CastFailure`. The error may include
column id, logical type names, batch sequence, and row offset inside the
batch. It must not include the cell value.

`CastFailurePolicy::SetNull`: unrepresentable values become null; the field
is nullable. The run continues.

E2 pauses `Cast` from `Date32` or `Timestamp` to `Utf8` (rule or `Expr`).
Date32 years fall outside `YYYY-MM-DD`, and Timestamp timezone strings are
unbounded, so a 10/35-byte ceiling is not a sound physical bound. Preflight
returns `TypeError`. Utf8 → Date32/Timestamp casts remain authorized under
the `Error` / `SetNull` policies. Binary has no `ScalarValue` variant;
`Literal(Binary)` does not exist. `FillNull` on a Binary column, and
`ReplaceLiteral` whose `from`/`to` is a non-null Binary value, are preflight
`TypeError`. Binary columns may only be projected, dropped, or passed
through. `ReplaceLiteral` on Binary may use `from = Null` and `to = Null`
only.

**Divide and modulo result types**

Let `T = least_upper_bound(left, right)`.

- If `T` is a signed or unsigned integer type: both operands are LUB-cast to
  `T` when needed. The result type is `T`. Integer division **truncates toward
  zero** (Rust `iN::div` / `uN` wrapping-forbidden division). Integer modulo
  is the Rust remainder: `a - (a / b) * b`, sign follows the dividend.
- If `T` is `Float32` or `Float64`: both operands are LUB-cast to `T`, then
  IEEE division/modulo is applied. A non-finite result is
  `EngineError::Arithmetic`. Silent `inf` / `NaN` is forbidden.
- Any other `T` is a preflight `TypeError`.

**Zero divisor** (integer or float) is `EngineError::Arithmetic` at the first
offending row. Same sanitization rules as casts.

**Overflow** is `EngineError::Arithmetic`, including:

- integer `MIN / -1` and unary negate of the signed minimum;
- integer add/subtract/multiply that cannot be represented in `T`.

No wrapping, saturating, or checked-to-null fallback is authorized for the
`Error` path.

## 12. Schema and Arrow propagation

Four distinct schemas exist. They must not be collapsed.

```text
Authorized source schema
  → expected connector envelope schema
  → engine-applied Scan projection
  → Scan output schema
```

### 12.1 Authorized source schema

If `schema_override` is present, it is the authorized **source** schema after
`LogicalSchema::validate()`. If absent, `preflight` calls
`ConnectorRegistry::inspect` once with an `InspectRequest` that carries the
same `RequestContext` and asset, then uses `AssetMetadata.schema`.

Inspect happens only after `context.ensure_active()` and after plan-shape /
binding checks that do not need the source schema. Join/Union/unsupported
rules must fail before inspect.

The authorized source schema must contain every `Scan.projection` id. Unknown
projection ids are `UnknownColumn`.

### 12.2 Expected connector envelope schema

- If `push_projection` is true: expected connector schema equals
  `Scan.projection` applied to the authorized source schema.
- If `push_projection` is false: expected connector schema equals the
  authorized source schema (full width).

Every connector envelope must match that expected schema by logical equality
and fingerprint. Drift is `ErrorCategory::SchemaDrift`. The engine must not
widen, rename, or coerce connector fields to keep the stream alive.

### 12.3 Scan output schema and later working schemas

If `push_projection` is true, Scan output schema equals the connector schema.
If false, the engine applies `Scan.projection` to each connector envelope;
Scan output schema is the projected schema.

Each later operator produces a new working `LogicalSchema` version 1:

- field order is meaningful;
- `ColumnId` values stay unique;
- display names stay unique and non-empty;
- metadata remains secret-free;
- schema resource ceilings from #30 remain in force
  (`MAX_SCHEMA_FIELDS`, `MAX_SCHEMA_NESTING_DEPTH`, `MAX_SCHEMA_TEXT_BYTES`).

The Materialize working schema is the Snapshot schema. Engine constructs one
`BatchEnvelopeFactory` from that schema and the bound `source_asset_id` and
reuses it for every output envelope.

### 12.4 Arrow 59

Output envelopes use `stillflow-core` canonical Arrow metadata:

- `stillflow.schema.version`
- `stillflow.schema.fingerprint`
- `stillflow.schema.metadata`
- `stillflow.column.id`
- `stillflow.field.metadata`

Engine must not write a second metadata vocabulary. Polars names are an
execution convenience and must be reconstructed from `LogicalSchema` before
`BatchEnvelopeFactory::try_from_batch`. Envelope byte accounting uses
`RecordBatch::get_array_memory_size()`, matching `BatchEnvelope`.

## 13. Determinism

A run is deterministic when all of the following hold.

1. Identical authorized input rows, identical validated plan, identical
   `batch_size`, and identical injected identities produce identical ordered
   logical output rows and an identical output `LogicalSchema`.
2. Changing only the connector's input batch partitioning must not change
   those logical rows, the schema, or `SnapshotStats.row_count`. With a fixed
   `batch_size`, the execution chunker (section 14.2) plus canonical rebatching
   (section 14.4) also keep output envelope boundaries and therefore
   `partition_count` unchanged.
3. The engine must not read random number generators, system clocks, locale,
   process id, or unordered `HashMap` iteration to decide row values, column
   order, envelope sequence, or fingerprints.
4. Injected `created_at` and `started_at` are the only timestamps written
   into storage calls. `RequestContext` deadlines may observe `Instant`
   solely to abort.
5. Canonical bytes of the plan remain the `stillflow-plan` fingerprint. The
   engine must not re-canonicalize with a different algorithm.

Non-determinism in a source file (for example concurrent mutation) is a
connector/`InvalidData` or `SchemaDrift` failure, not an engine license to
reorder rows.

## 14. Resource bounds

Every limit below is a hard ceiling. Exceeding it fails the run with a typed
error. No unbounded collect, prefetch queue, or full-source materialization
is authorized.

E2 must not send a whole connector envelope through Polars and split afterwards.
Output expansion is predicted and chunked **before** Polars allocation:

```text
Connector envelope
  → deterministic execution chunker
  → Polars transform of one chunk
  → canonical rebatcher
  → SnapshotWriter
```

### 14.1 Live payloads and peak

Live engine memory is exactly this set:

```text
connector envelope
  + complete Polars working set
  + canonical remainder
  + bounded non-columnar state
```

Those three columnar payloads may coexist. That is required when one envelope
is split into two or more execution chunks while a remainder from the first
chunk is still held. Remainder → output envelope is a **move/freeze** of the
remainder builder, not a second copy.

| Resource | Ceiling | Source |
| --- | --- | --- |
| Input envelope rows | `MAX_BATCH_ROWS` = 65,536 | `stillflow-core` |
| Input envelope Arrow bytes | `MAX_BATCH_BYTES` = 64 MiB | `stillflow-core` |
| Complete Polars working set (old + new + temporary) | `MAX_BATCH_BYTES` | this contract |
| Canonical remainder | `MAX_BATCH_BYTES` | this contract |
| Output envelope (moved remainder) | `MAX_BATCH_BYTES` | same |
| Request `batch_size` | `1..=ReadRequest::MAX_BATCH_SIZE` (65,536) | `stillflow-core` |
| Simultaneously live columnar payloads | `MAX_LIVE_COLUMNAR_PAYLOADS` = 3 | this contract |
| Compiled plan + maps | `MAX_COMPILED_PLAN_BYTES` = 4 MiB | this contract |
| FFI scratch (C ABI structs, not buffers) | `MAX_FFI_SCRATCH_BYTES` = 1 MiB | this contract |
| Operator extra state | `MAX_OPERATOR_STATE_BYTES` = 5 MiB | compiled plan + FFI scratch + predicted-column table |
| Peak live **engine** bytes | `MAX_ENGINE_PEAK_BYTES` = 197 MiB | `3 * 64 MiB + 5 MiB` |
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
O(connector_envelope + polars_working_set + remainder + bounded_operator_state)
peak ≤ MAX_ENGINE_PEAK_BYTES = 197 MiB
live columnar payloads ≤ 3
each of those three ≤ MAX_BATCH_BYTES
```

Permitted coexistence (the T37 case):

- unconsumed connector envelope
- complete Polars working set of the current execution chunk
- canonical remainder from previous chunks of this or a prior envelope

Forbidden: a fourth `MAX_BATCH_BYTES`-class copy. Flushing remainder to an
output envelope **moves** the remainder allocation; after the move the
remainder builder is empty. `SnapshotWriter::append` may then borrow that
one envelope. Parquet encode buffers allocated inside `stillflow-storage`
are **not** part of `MAX_ENGINE_PEAK_BYTES`. T23's engine allocator must
exclude them; T44 measures them in a separate phase.

`MAX_OPERATOR_STATE_BYTES` (5 MiB) is compiled expressions, working schemas,
ColumnId maps, the predicted-column table (section 14.3), and C ABI structs.
It does not include the three columnar payloads.

Lifecycle:

1. Hold the connector envelope until every execution chunk from it has been
   transformed and fed to the rebatcher, then drop it.
2. Hold at most one Polars working set. Drop it before importing the next
   chunk. Do not drop the envelope or the remainder to make room for Polars.
3. Remainder survives across chunks and across envelopes until it is moved
   into an output envelope or the stream ends.
4. After `append` returns, drop the output envelope. Do not keep remainder
   and output as two allocations.

If `RequestContext` has no deadline, the engine applies
`ENGINE_DEFAULT_DEADLINE` from the start of `materialize`. A caller-supplied
deadline longer than `ENGINE_MAX_DEADLINE` is rejected in preflight.

### 14.2 Deterministic execution chunker

The chunker runs on each connector envelope **before** Arrow→Polars import of
that subset. It must not call Polars to measure size.

**Source-slice accounting** (not `get_array_memory_size` of a shared parent):

For a row slice `[i, i + k)` of one **source** Arrow array:

- validity: `(k + 7) / 8` bytes if a bitmap exists, else 0;
- fixed-width values: `k * slot_size`;
- Utf8/Binary values: `utf8_physical_bytes(k, offsets[i + k] - offsets[i])`
  defined in section 14.3;
- nested list/struct: the same rules applied recursively to child arrays
  over the sliced element range.

A zero-copy Arrow slice that shares a parent buffer does **not** count the
unused parent bytes a second time. Shared parent bytes already counted in
the live connector envelope remain attributed to that envelope until it is
dropped. The Polars working set is a separate payload and is bounded by
`predict(k)`.

**Algorithm**, deterministic maximum feasible prefix:

Let `n` be the envelope row count and `offset` the first unconsumed row
(initially 0). While `offset < n`:

1. Let `predict(k)` be the peak Polars working-set upper bound from
   section 14.3 for rows `[offset, offset + k)`.
2. If `predict(1) > MAX_BATCH_BYTES`, fail with `BoundExceeded` **before**
   Arrow→Polars import and before any `with_columns` / `lit` materialization.
   Never split a row by column.
3. Let `k` be the largest integer in `1..=(n - offset)` such that
   `predict(k) <= MAX_BATCH_BYTES`.
4. Import only that slice into Polars, transform it, export it, feed the
   canonical rebatcher, then drop the Polars frame before the next slice.
   The envelope and remainder remain live.

Identical input rows, plan, and `batch_size` therefore still produce identical
output envelope boundaries: the chunker is a function of schema, literals,
predicted-column state, and Arrow offsets, not of wall-clock or hashmap order.

### 14.3 Predicted expansion before allocation

The engine maintains an ordered **predicted-column table** for the working
schema, one entry per `ColumnId`:

```text
PredictedColumn {
    id, name, data_type,
    nullable,                 // working-schema flag; section 11.4 inference updates this
    max_value_bytes,          // per-value data ceiling for Utf8/Binary; 0 for fixed-width
    origin: Source { ordinal } | Derived
}
```

Source columns use Arrow offset/validity buffers of the current envelope
slice. Derived columns **do not** have source offsets. After `DeriveColumn`,
later Trim/Replace/FillNull/Cast on that id use `max_value_bytes` and
`nullable` from this table only. Drop removes the entry. Rename changes
`name` only.

UTF-8/Binary physical size, including view, offset, and validity overhead:

```text
utf8_physical_bytes(k, data_bytes) =
    data_bytes
    + k * UTF8_VIEW_SLOT_BYTES          // 16; Polars 0.46 Utf8View/BinaryView
    + (k + 1) * UTF8_OFFSET_SLOT_BYTES  // 4; Arrow Utf8 i32 offsets during FFI
    + (k + 7) / 8                       // validity
```

Fixed-width physical size:

```text
fixed_physical_bytes(k, slot) = k * slot + (k + 7) / 8
```

`predict(k)` is the maximum, over every pipeline step after Scan projection,
of:

```text
predict_step(k) = live_before(k) + temporary_allocation(k) + live_after(k)
predict(k)      = max over steps of predict_step(k)
```

That sum is the complete Polars working-set bound for one chunk and must be
`<= MAX_BATCH_BYTES`. It is not a tight Polars measurement; it is a strict
upper bound that covers old-frame / new-frame coexistence.

`live_before` is the sum of `column_physical_bytes` for every column in the
working schema at the start of the step. `temporary_allocation` is every new
array Polars may allocate before dropping the old ones (replacement
operators keep the old column live until the new array exists). `live_after`
is the working schema after the step.

If an actual Polars working set exceeds `MAX_BATCH_BYTES`, that is
`EngineError::Internal` (the predictor under-estimated) and the frame is
dropped without append.

**Column physical bytes**

| Column origin | Bytes for `k` rows |
| --- | --- |
| Source, fixed-width | `fixed_physical_bytes(k, slot)` |
| Source, Utf8/Binary | `utf8_physical_bytes(k, offsets[i+k]-offsets[i])` |
| Derived, fixed-width | `fixed_physical_bytes(k, slot)` |
| Derived, Utf8/Binary | `utf8_physical_bytes(k, k * max_value_bytes)` |

**`temporary_allocation` by operator**

| Step | `temporary_allocation(k)` |
| --- | --- |
| Rename | 0 (metadata only) |
| Project / Drop | 0 if zero-copy column pointers; otherwise the physical size of kept columns (never more than `live_before`) |
| Filter / FilterRows | `live_before` (worst-case copy of surviving rows; `live_after ≤ live_before`) |
| Trim | new Utf8 array using the **input** column's `data_bytes` ceiling (trim does not grow) plus view/offset/validity |
| Cast to fixed-width | `fixed_physical_bytes(k, dest_slot)` |
| Cast to Utf8 from Boolean / integer / float / Utf8 | `utf8_physical_bytes(k, k * per_value_ceiling)` |
| Cast to Utf8 from Date32, Timestamp, Binary, List, Struct | preflight `TypeError` |
| Cast to Binary from non-Binary | preflight `TypeError` |
| DeriveColumn | physical size of the new column from its expression bound |
| ReplaceLiteral to Utf8 | `utf8_physical_bytes(k, k * max(input_max_value_bytes, len(to)))` |
| ReplaceLiteral to Null | 0 extra data; validity may be rewritten: `(k + 7) / 8` |
| FillNull Utf8 | `utf8_physical_bytes(k, input_data_bytes + null_count * len(value))` |
| FillNull numeric/bool | `fixed_physical_bytes(k, slot)` |
| FillNull Binary | preflight `TypeError` |

Per-value UTF-8 data ceilings (`max_value_bytes` after the step):

| Producer | `max_value_bytes` |
| --- | --- |
| `Literal(Utf8)` | exact UTF-8 length |
| `Literal` numeric/bool/null | 0 (fixed-width result) |
| `Column` Utf8 source | `max` value length in the current Arrow slice |
| `Column` Utf8 derived | that column's `max_value_bytes` |
| `Cast` to Utf8 from Boolean | `MAX_BOOL_UTF8_BYTES` = 5 |
| `Cast` to Utf8 from any integer | `MAX_INT_UTF8_BYTES` = 20 |
| `Cast` to Utf8 from float | `MAX_FLOAT_UTF8_BYTES` = 32 |
| `Cast` to Utf8 from Utf8 | input `max_value_bytes` |
| `Coalesce` | max of arms |
| Boolean comparisons, `And`/`Or`, `IsNull`, `Contains` | 0 (1-byte Boolean column) |

`null_count` for a **source** Utf8 column is read from the Arrow validity
bitmap of the slice. For a **derived** column, `null_count` is `k` if
`nullable` else `0` (conservative). `input_max_value_bytes` for a derived
Utf8 column is its table `max_value_bytes`, never an Arrow offset.

After each step the table is updated: Derive appends an entry; Drop removes
one; Trim does not increase `max_value_bytes`; ReplaceLiteral to Utf8 sets
`max_value_bytes = max(old, len(to))` and `to = Null` sets `nullable = true`;
FillNull Utf8 sets `nullable = false` and
`max_value_bytes = max(old, len(value))`.

A 2 KiB derived UTF-8 literal on 65,536 rows therefore cannot select
`k = 65,536`, because `predict_step` includes `live_before + utf8_physical_bytes(k, k * 2048)`
and that exceeds `MAX_BATCH_BYTES`. The chunker must pick a smaller `k`.
Remainder from the first chunk remains live while the second chunk's Polars
frame is built (three payloads).

### 14.4 Canonical rebatching

Byte accounting for the **remainder builder** uses the same
`utf8_physical_bytes` / `fixed_physical_bytes` rules, not a second copy of
the Polars frame. Export from Polars into the remainder may append by move of
finished arrays.

Let `pack_limit` be `batch_size` rows. Incoming transformed execution chunks
feed one remainder builder.

For each incoming row-group `G` with `n` rows, in order:

1. Let `k` be the **largest** integer in `0..=n` such that
   `remainder.rows + k <= pack_limit` **and**
   `bytes(remainder concatenated with G[0..k]) <= MAX_BATCH_BYTES`.
2. If `k > 0`, append `G[0..k]` to remainder. If remainder now meets
   `pack_limit` rows or cannot accept another row of `G` without exceeding
   the byte cap, **freeze/move** remainder into one output envelope
   (sequence += 1), append it, drop that envelope after `append` returns,
   then continue with `G[k..]` against an empty remainder.
3. If `k == 0` and remainder is non-empty, freeze/move remainder, then retry
   `G` against the empty remainder.
4. If `k == 0` and remainder is empty, the next single row exceeds
   `MAX_BATCH_BYTES`. Fail with `BoundExceeded`. Never split a row by
   column.

After the stream ends, freeze/move a non-empty remainder as the final
envelope.

This search for `k` is deterministic (maximum feasible prefix). It does not
use sampling, hash order, or wall-clock.

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

All `SnapshotDraft` and `begin_snapshot` fields are caller-injected through
`ExecutionIdentities` plus the bound asset and Materialize schema:

| Field | Source |
| --- | --- |
| `id` | `identities.snapshot_id` |
| `dataset_id` | `identities.dataset_id` |
| `session_id` | `identities.session_id` |
| `source_asset_id` | bound `SourceAsset.id` |
| `schema` | Materialize working schema |
| `lineage` | `identities.lineage` (empty set is valid) |
| `quality_score` | `identities.quality_score` (`None` or `0..=100`) |
| `created_at` | `identities.created_at` |
| `started_at` (begin_snapshot) | `identities.started_at` |

Invariants, checked before `begin_snapshot`:

- none of `snapshot_id`, `dataset_id`, `session_id`, `source_asset_id` is nil;
- no lineage id is nil;
- `quality_score` is `None` or `<= 100`;
- `created_at <= started_at` (matches `SnapshotStore::begin_snapshot`).

The engine must not call `Uuid::new_v4()` or `Utc::now()` for these fields.
It must not default `lineage` or `quality_score` when the caller omitted
them: they are struct fields, not `Option` wrappers around the request.
Callers who want empty lineage and unknown quality pass `BTreeSet::new()`
and `None` explicitly.

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

`EngineError` is an internal typed failure. It must **not** implement
`Serialize` / `Deserialize`. The frozen public surface is:

```rust
impl EngineError {
    pub fn category(&self) -> ErrorCategory;
    pub fn retryable(&self) -> bool;
    pub fn sanitized_summary(&self) -> SanitizedErrorSummary;
}
```

`sanitized_summary()` must call `SanitizedErrorSummary::try_new(category,
retryable, message)` using a message that already excludes forbidden content.
E2 must not emit events that carry `EngineError` itself. Any event, log line,
or later API body uses `SanitizedErrorSummary` only. T13/T18/T35 serialize
that summary (and optional event metadata wrapping it), not `EngineError`.

`Display` is the sanitized user message. `Debug` may include variant name,
node id, column id, counts, and limits. Neither Display, Debug, nor
`sanitized_summary().message()` may contain cell values, `output_label`,
credentials, paths, SQL, or Polars backtraces.

Allowed message contents: node id, column id, field name, operator/rule kind,
logical type names, counts, limits, batch sequence, row offset.

### 16.1 Category and retryability

| `EngineError` | `ErrorCategory` | `retryable` |
| --- | --- | --- |
| `UnsupportedOperator` | `UnsupportedCapability` | false |
| `UnsupportedRule` | `UnsupportedCapability` | false |
| `UnsupportedCapability` | `UnsupportedCapability` | false |
| `SourceBinding` | `InvalidConfiguration` | false |
| `InvalidPlan` | `InvalidConfiguration` | false |
| `UnknownColumn` | `InvalidConfiguration` | false |
| `TypeError` | `InvalidData` | false |
| `CastFailure` | `InvalidData` | false |
| `Arithmetic` | `InvalidData` | false |
| `SchemaDrift` | `SchemaDrift` | false |
| `BoundExceeded` | `InvalidData` | false |
| `Ffi` | `Internal` | false |
| `Cancelled` | `Cancelled` | false |
| `Timeout` | `Timeout` | true |
| `Busy` | `RateLimited` | true |
| `Connector(inner)` | `inner.category()` | `inner.retryable()` |
| `Storage(inner)` | section 16.2 | section 16.2 |

Names of variants may differ; each row must exist and be tested.

### 16.2 Storage mapping

| `StorageError` | `ErrorCategory` | `retryable` |
| --- | --- | --- |
| `Busy` | `RateLimited` | true |
| `NotFound` | `NotFound` | false |
| `SchemaDrift` | `SchemaDrift` | false |
| `InvalidConfiguration`, `InvalidDraft`, `UnsupportedStorageVersion`, `AlreadyExists`, `InvalidTimestampOrder`, `InvalidManifest`, `Snapshot` | `InvalidConfiguration` | false |
| `Sequence`, `LineageMismatch`, `EnvelopeLimitExceeded`, `PartitionLimitExceeded`, `RowLimitExceeded`, `StoredByteLimitExceeded` | `InvalidData` | false |
| `ArithmeticOverflow`, `Integrity`, `Io`, `Database`, `Parquet`, `Serialization`, `Batch`, `ActivityState` | `Internal` | false |

Storage user messages are the existing `Display` strings. They already omit
cell values and credentials. The engine must not append paths or IO details
beyond that `Display`.

`Connector` wrappers reuse `ConnectorError::sanitized_summary()` category,
retryable flag, and message.

### 16.3 Required variants

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
- `BoundExceeded` (rows, bytes, nodes, time, concurrency, live buffers, predicted expansion)
- `Ffi`
- `Cancelled`
- `Timeout`
- `Busy`
- `Storage`
- `Connector`

`EngineError` must not implement `Display` by forwarding unsanitized
third-party strings.

## 17. E2 vertical slice

After this contract is approved, E2 rebuilds from the latest `main` and
implements one vertical path only:

```text
Connector BatchStream
  -> LogicalPlan preflight (async, with RequestContext)
  -> deterministic execution chunker
  -> Stateless Polars lowering of one chunk
  -> Canonical output batching
  -> BatchEnvelopeFactory
  -> SnapshotWriter
  -> Atomic commit
```

E2 tests must cover the table in section 19.2. E2 must not implement the
remaining rule kinds, Preview HTTP, or job runtime. E2 must not lower a full
connector envelope through Polars before chunking.

## 18. Implementation checklist

This checklist is for the later E2 PR. This docs PR only records it.

1. Add `ConnectorRegistry::capabilities` as specified in section 6.4.
2. Add `stillflow-plan` and `stillflow-storage` to `stillflow-engine`.
3. Add Polars 0.46 / `polars-arrow` 0.46 / Arrow FFI crates as specified.
4. Implement private bidirectional FFI bridge with size/alignment asserts and
   partial-import cleanup.
5. Implement `EngineError` with `category` / `retryable` / `sanitized_summary`
   only. Do not serialize `EngineError`.
6. Implement async `preflight` with context, inspect, and capability checks.
7. Implement the per-instance run gate and `materialize` so it `try_acquire`
   before preflight/inspect and always re-runs `preflight`.
8. Implement Scan read through `ConnectorRegistry` only, with the four schema
   stages.
9. Implement the execution chunker (section 14.2–14.3) and stateless lowering
   for the five nodes and eight rules.
10. Implement canonical rebatching (section 14.4) and one
    `BatchEnvelopeFactory`.
11. Construct `SnapshotDraft` from injected identities, including lineage,
    quality_score, and `started_at`.
12. Add the automated tests listed in section 19.
13. Run repository checks. Do not include Dependabot diffs.

## 19. Acceptance criteria

### 19.1 This docs PR

- Diff contains only Issue #46, this contract, and necessary roadmap updates.
- No Rust runtime file, `Cargo.toml`, `Cargo.lock`, frontend file, or workflow
  file is modified.
- Every memory, row, byte, time, and concurrency limit in section 14 has an
  explicit numeric ceiling, including three live columnar payloads and
  `MAX_ENGINE_PEAK_BYTES` = 197 MiB.
- Every criterion in section 19.2 is written so E2 can automate it.
- Dependency arrows in section 6.1 match ADR-001 and `AGENTS.md`.
- Work stops after architecture review. E2 does not start in this PR.

### 19.2 E2 automated tests (must be expressible now)

The sanitization sentinel is the UTF-8 string
`STILLFLOW_SENTINEL_CELL_VALUE_9f3c2a`. It must appear as a cell value in the
failing fixture and must not appear in `EngineError` Display/Debug,
`sanitized_summary()` JSON, or event metadata wrapping that summary.

| ID | Criterion | Automated evidence |
| --- | --- | --- |
| T01 | Linear `Scan -> Project -> Filter -> ApplyRules -> Materialize` materializes | fixture snapshot row_count and schema |
| T02 | Two input partitionings of the same rows yield equal logical rows and stats.row_count | compare collected columns |
| T03 | Fixed `batch_size` yields equal output envelope boundaries, including byte-cap splits | compare sequences and per-envelope row_count |
| T04 | `Join` preflight is `UnsupportedOperator`; no snapshot | error category + `load_manifest` fails |
| T05 | `Union` preflight is `UnsupportedOperator`; no snapshot | same |
| T06 | `Rule::Validate` / `Rule::Deduplicate` preflight is `UnsupportedRule` | same |
| T07 | Scan id mismatch is `SourceBinding` before stream | mock registry poll-count = 0 |
| T08 | Connector schema drift against the **expected connector schema** aborts and publishes nothing | `SchemaDrift` + no manifest |
| T09 | Cancel before `read_batches` publishes nothing | `Cancelled` + no manifest |
| T10 | Cancel during lowering publishes nothing | same |
| T11 | Cancel after append, before commit, publishes nothing | same |
| T12 | Deadline before commit publishes nothing | `Timeout` + no manifest |
| T13 | Cast `Error` fails without embedding the cell sentinel | sentinel absent from `EngineError` Display/Debug and from `serde_json` of `sanitized_summary()` and of a constructed event metadata map; `EngineError` itself does not implement Serialize |
| T14 | Cast `SetNull` writes null and continues | null count |
| T15 | Trim / ReplaceLiteral / FillNull / DropColumn / Rename / DeriveColumn / FilterRows match fixtures | golden logical rows |
| T16 | Unknown `ColumnId` is `UnknownColumn` | typed error |
| T17 | Incomparable `Expr` types are `TypeError` at preflight | no stream |
| T18 | Division by zero is `Arithmetic` without the cell sentinel | same surfaces as T13; `category() == InvalidData` and `retryable() == false` |
| T19 | Engine crate does not depend on adapter crates | `cargo tree -p stillflow-engine` stdout contains none of `stillflow-connector-local-tabular`, `stillflow-connector-workbook`, `stillflow-connector-object-store` |
| T20 | `stillflow-engine` depends on plan, connectors, storage, core | `cargo tree -p stillflow-engine` |
| T21 | Injected snapshot/session/dataset ids, `created_at`, `started_at`, lineage, and quality_score appear unchanged | manifest equality |
| T22 | Engine does not call `Uuid::new_v4` / `Utc::now` on the materialize path | source grep of engine runtime excluding tests |
| T23 | Peak live payloads and engine bytes, including expansion | instrumented live-payload counter `<= 3` while streaming `>= 4` batches and while one envelope is split into `>= 2` chunks with remainder retained; engine test allocator `<= MAX_ENGINE_PEAK_BYTES` (197 MiB) and excludes `SnapshotWriter` Parquet encode/write scratch; grep forbids collecting the full connector stream **and** forbids importing a full envelope into Polars when `predict(n) > MAX_BATCH_BYTES`. Source grep alone is not sufficient. Adversarial fixtures in T37/T41 |
| T24 | Fifth concurrent `materialize` on one engine is `Busy` | four in-flight runs hold the gate; fifth returns `category() == RateLimited`, `retryable() == true`; mock inspect count 0 and read poll count 0 |
| T25 | Empty source commits a zero-row snapshot | stats conservation |
| T26 | `cargo fmt --all -- --check` | CI |
| T27 | `cargo clippy --workspace --all-targets -- -D warnings` | CI |
| T28 | `cargo test --workspace` | CI |
| T29 | `npm run typecheck` and `npm run build` | CI, unchanged frontend |
| T30 | `materialize` still rejects Join when the caller previously obtained any other `PreparedPlan` | calling `materialize` with a Join plan errors; no snapshot |
| T31 | Missing schema override plus cancelled context fails before inspect completes | mock inspect not started or cancelled |
| T32 | No `ColumnProjection`: connector full-width schema accepted; Scan output is projected | schema fingerprints of connector vs Scan output differ; rows match projection |
| T33 | `ReplaceLiteral` with `to = Null` makes the field nullable | output schema + null row |
| T34 | Integer `8 / 3 == 2` (toward-zero); `Int8 MIN / -1` is `Arithmetic` | fixtures |
| T35 | Secret-like `output_label` is `InvalidPlan`; successful errors omit the label | preflight + Display/Debug + `sanitized_summary()` JSON |
| T36 | Mid-schema Arrow→Polars import failure releases all exports and publishes nothing | drop counters + no manifest |
| T37 | Derive a 2 KiB UTF-8 literal over a 65,536-row connector batch | snapshot has 65,536 rows; live Polars working set never exceeds `MAX_BATCH_BYTES`; chunker `k < 65,536`; envelope + Polars working set + remainder may be live together; peak `<= MAX_ENGINE_PEAK_BYTES`; no fourth `MAX_BATCH_BYTES` copy |
| T38 | `ReplaceLiteral` to a 2 KiB string and `FillNull` with a 2 KiB string over 65,536 rows | same peak/chunker assertions as T37; logical values match |
| T39 | Single-row predicted expansion `> MAX_BATCH_BYTES` fails before Polars import | mock FFI/Polars import count 0; `BoundExceeded`; no snapshot |
| T40 | Section 16.1 mapping | table-driven: each variant's `category()` and `retryable()`; `Busy` is RateLimited/true; `Timeout` is Timeout/true; `Cancelled` is Cancelled/false |
| T41 | One connector envelope is split into at least two execution chunks while remainder from the first chunk is still live | live-payload counter shows envelope + Polars working set + remainder together; no fourth `MAX_BATCH_BYTES` copy; snapshot `row_count` matches input |
| T42 | DeriveColumn, then Drop the source column, then Trim and ReplaceLiteral on the derived column | logical rows match fixture; predictor uses `PredictedColumn` width/nullability, not source Arrow offsets; peak `<= MAX_ENGINE_PEAK_BYTES` |
| T43 | UTF-8 values whose data bytes sit exactly on the predicted byte cap, including view/offset/validity overhead | chunker `k` satisfies `predict(k) <= MAX_BATCH_BYTES < predict(k+1)` (or `BoundExceeded` with import count 0 when `predict(1)` exceeds the cap) |
| T44 | Allocator peaks measured in three isolated phases | (a) Polars working set of one chunk; (b) remainder builder / freeze-move; (c) storage `append` Parquet encode. `(a)+(b)+5 MiB <= MAX_ENGINE_PEAK_BYTES`; (c) is recorded and excluded from the engine ceiling |
| T45 | `Cast` Date32 or Timestamp → Utf8 (rule or `Expr`) | preflight `TypeError`; mock inspect/read counts 0; no snapshot |

## 20. Stop conditions

Stop and return to contract review if implementation needs:

- a public type not named in section 7;
- a dependency not named in section 6.3;
- a registry method other than `capabilities` in section 6.4;
- engine imports of a connector adapter crate;
- a second cleaning semantics in DuckDB or SQL;
- unbounded collect, prefetch, or full-source materialization;
- generated snapshot/session/dataset ids, timestamps, lineage, or quality;
- Join/Union/Validate/Deduplicate execution;
- a compatibility shim;
- Dependabot or unrelated lockfile edits;
- a message that includes a cell value, credential, or `output_label`;
- serializing `EngineError` instead of `SanitizedErrorSummary`;
- lowering a full connector envelope through Polars when predicted expansion
  exceeds `MAX_BATCH_BYTES`;
- a two-buffer / 133 MiB peak, or dropping the connector envelope before its
  last execution chunk in order to keep a 2-slot columnar budget.

## 21. Known risks

- Two FFI bridges can drift. Engine tests must cover Null-type buffer layout,
  both directions, and partial-import cleanup independently of the connector
  crate.
- Polars name-based execution can collide if rename/derive uniqueness checks
  are skipped. Preflight uniqueness is mandatory.
- Canonical rebatching interacts with `MAX_BATCH_BYTES`. A single wide row
  that exceeds the byte cap fails; it must not be silently split by column.
- Variable-width Derive/Replace/FillNull can expand far beyond the connector
  envelope. The execution chunker is mandatory; “transform then split” cannot
  meet `MAX_ENGINE_PEAK_BYTES`.
- A two-buffer / 133 MiB model cannot implement T37: a split envelope, the
  current Polars working set, and a canonical remainder must coexist. Peak
  engine bytes are therefore `3 * 64 MiB + 5 MiB`. Remainder → output is
  move/freeze; a fourth `MAX_BATCH_BYTES` copy is `Internal`.
- `predict(k)` is `live_before + temporary_allocation + live_after` per step.
  Derived columns use `PredictedColumn` width/nullability, never source Arrow
  offsets.
- Date32/Timestamp → Utf8 is paused until a proven maximum format length
  exists for the full physical domain. E2 must not invent a 10/35-byte cap.
- RSS measurements are noisy. T23/T37/T41/T44 primary evidence is the
  live-payload counter, predicted-k assertions, and phased test allocator,
  not host RSS.
- `MAX_ENGINE_CONCURRENT_RUNS` is lower than storage publisher capacity so
  tests can saturate the engine cap without depending on storage busy errors.
- SQL Connector #9 and DuckDB #10 remain outside this contract. They must not
  be pulled forward to make a type convenient.
