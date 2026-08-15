# Issue #50 Implementation Contract: node-level Preview (E3-C0)

> Status: Frozen for architecture review (not approved)
> Revision: C0-R1
> Supersedes: C0 at `a57589edc160739130b3d5375134c003613b9d0e`
> Risk: High
> Issue: #50
> Parent contract: Issue #46 revision R3, merged at
> `32f1c53d9903f66aeaca1c2676c0b81abfb2a702` in PR #47
> Authorized base: `main@4b65204cfdb69c73389fba77cf4fd9715e94cba`
> Branch: `agent/issue-050-node-preview-contract`
> Last updated: 2026-08-15
> Review: PR #51 remains draft with Request changes. C0 at
> `a57589edc160739130b3d5375134c003613b9d0e` was not approved. This C0-R1
> revision resolves P0-1/P0-2/P0-3; architecture approval binds exactly one
> new commit SHA of this file. Runtime implementation starts only after
> that approval.

This document freezes the public contract and objective acceptance matrix for
Engine E3-C0 node-level Preview. It does **not** authorize any Rust runtime
code, dependency change, lockfile change, CI change, or frontend change.

## 1. Objective

Freeze the complete contract for executing a validated `LogicalPlan` only up
to one caller-selected `target_node_id` and returning a bounded, deterministic,
read-only Preview result. The first E3 phase supports `Scan`, `Project`,
`Filter`, and `ApplyRules` target nodes. It must never execute `Materialize`
or publish a Snapshot.

E3-C0 must reuse the E2 preflight, typing, lowering, execution chunker, Arrow
interchange, error taxonomy, and sanitized-error semantics from Issue #46.
It must not create a second preview executor or a second cleaning/typing
semantics.

## 2. Source policy

- The authorized base is the latest accepted `main` commit shown in the
  header. E3 runtime must later rebuild from the latest accepted `main` at
  that time.
- This branch is created from `main@4b65204`. It must not be based on,
  rebased onto, cherry-picked from, or otherwise modified through PR #49.
- PR #49 and all historical/Dependabot branches are read-only references.
- This delivery is docs-only. It must not modify Rust sources, `Cargo.toml`,
  `Cargo.lock`, frontend files, CI workflows, or any file other than the
  files authorized in section 4.

## 3. Risk and compatibility

This work is `risk:high` because it defines a new public engine API, target
cutoff semantics, truncation reporting, response-buffer memory law,
concurrency sharing, deadline policy, and read-only guarantees for all later
E3 deliveries.

Compatibility decision:

- No compatibility shim is provided for `Join`, `Union`, `Materialize` as a
  preview target, `Rule::Validate`, `Rule::Deduplicate`, DuckDB SQL, SQLx, or
  arbitrary engine code.
- `LogicalPlan`, `PlanNodeId`, `Rule`, `Expr`, `BatchEnvelope`,
  `LogicalSchema`, `RequestContext`, `SourceConnection`, and `SourceAsset`
  version 1 contracts remain unchanged.
- E3 may later add only the public types and the one public method named in
  section 5. No other public core, plan, connector, storage, or engine type
  may change.
- `PreviewRequest` and `PreviewResult` must not expose Polars types. The only
  batch payload type crossing the public boundary is `BatchEnvelope`.

## 4. In scope for this contract

Documentation of:

- Public `PreviewRequest` / `PreviewResult` fields, constants, and invariants.
- `target_node_id` cutoff, full-plan preflight, and supported target kinds.
- Reuse of E2 preflight, typing, lowering, chunker, rebatching accounting,
  and sanitized errors.
- Exact row, byte, deadline, and concurrency ceilings.
- Deterministic earliest-prefix truncation and the four reporting flags plus
  two source-scan counters.
- Preview response-buffer memory model and peak law.
- Read-only guarantees: no Snapshot, no `SnapshotWriter`, no generated IDs or
  timestamps, no partial-result publication.
- Cancellation and deadline observation points.
- Schema and `ColumnId` propagation through the target boundary.
- Connector inspect/read call-count rules.
- Objective acceptance tests P01–P15 for the later E3 runtime PR.

## 5. Explicit non-goals

This contract and the later E3 implementation it authorizes must not include:

- API routes, Axum handlers, HTTP status machines, or job tables (E5).
- Frontend layout, components, CSS, tokens, or generated types.
- DuckDB, SQLx, ConnectorX, or SQL Connector #9.
- Execution of `Join`, `Union`, or `Materialize`.
- `Rule::Validate`, `Rule::Deduplicate`, or Rejected Rows datasets (E4).
- Snapshot creation, `SnapshotWriter` calls, Parquet writes, SQLite writes,
  or any other visible publication.
- Generated `Uuid`, `DateTime`, plan node IDs, column IDs, lineage IDs, or
  quality scores. All identities come from the caller or the validated plan.
- AI execution, Python, SQL strings, or arbitrary Polars/Python/SQL programs.
- A second run gate, an unbounded task queue, or a waiting/queued preview
  entry point.
- Sampling, reservoir, random selection, or any non-prefix row selection.
- Dependabot updates mixed into the E3 branch.
- Historical branch merge, rebase, or cherry-pick.

## 6. Public API

Names may be organized into modules. Semantics and field order must match
this section. E3 implements these types in `stillflow-engine` later; this
docs PR must not add them.

```rust
use std::time::Duration;

pub const PREVIEW_DEFAULT_ROW_LIMIT: usize = 1_000;
pub const PREVIEW_MAX_ROW_LIMIT: usize = 10_000;
pub const PREVIEW_DEFAULT_BYTE_LIMIT: usize = 8 * 1024 * 1024;
pub const PREVIEW_MAX_BYTE_LIMIT: usize = 50 * 1024 * 1024;
pub const PREVIEW_DEFAULT_BATCH_SIZE: usize = 1_024;
pub const PREVIEW_MAX_SOURCE_ROWS_SCANNED: usize = 100_000;
pub const PREVIEW_MAX_SOURCE_BYTES_SCANNED: usize = MAX_BATCH_BYTES; // 64 MiB
pub const PREVIEW_DEFAULT_DEADLINE: Duration = Duration::from_secs(30);
pub const PREVIEW_MAX_DEADLINE: Duration = Duration::from_secs(30);
pub const PREVIEW_MAX_CONCURRENT_REQUESTS: usize =
    MAX_ENGINE_CONCURRENT_RUNS as usize;
pub const PREVIEW_RESPONSE_MAX_BYTES: usize = PREVIEW_MAX_BYTE_LIMIT;
pub const PREVIEW_PEAK_ENGINE_BYTES: usize =
    MAX_BATCH_BYTES + MAX_BATCH_BYTES + PREVIEW_RESPONSE_MAX_BYTES
        + MAX_OPERATOR_STATE_BYTES; // 64 MiB + 64 MiB + 50 MiB + 5 MiB = 183 MiB

pub struct PreviewRequest {
    pub plan: LogicalPlan,
    pub target_node_id: PlanNodeId,
    pub connection: SourceConnection,
    pub asset: SourceAsset,
    pub schema_override: Option<LogicalSchema>,
    pub context: RequestContext,
    pub batch_size: usize,
    pub row_limit: usize,
    pub byte_limit: usize,
}

impl PreviewRequest {
    pub fn new(
        plan: LogicalPlan,
        target_node_id: PlanNodeId,
        connection: SourceConnection,
        asset: SourceAsset,
    ) -> Self {
        Self {
            plan,
            target_node_id,
            connection,
            asset,
            schema_override: None,
            context: RequestContext::default(),
            batch_size: PREVIEW_DEFAULT_BATCH_SIZE,
            row_limit: PREVIEW_DEFAULT_ROW_LIMIT,
            byte_limit: PREVIEW_DEFAULT_BYTE_LIMIT,
        }
    }
}

pub struct PreviewResult {
    pub plan_fingerprint: PlanFingerprint,
    pub target_node_id: PlanNodeId,
    pub schema: LogicalSchema,
    pub batches: Vec<BatchEnvelope>,
    pub rows_returned: usize,
    pub bytes_returned: usize,
    pub source_rows_scanned: usize,
    pub source_bytes_scanned: usize,
    pub rows_truncated: bool,
    pub bytes_truncated: bool,
    pub scan_truncated: bool,
    pub source_exhausted: bool,
}

impl ExecutionEngine {
    pub async fn preview(
        &self,
        request: PreviewRequest,
    ) -> Result<PreviewResult, EngineError>;
}
```

`PREVIEW_MAX_CONCURRENT_REQUESTS` is a direct alias of the E2 run-gate
capacity `MAX_ENGINE_CONCURRENT_RUNS`; no literal `4` is repeated in the
public contract. E3 must not create a second semaphore or any other
admission-control primitive.

`PREVIEW_PEAK_ENGINE_BYTES` is a hard ceiling. The three columnar payloads
are one connector envelope (`<= MAX_BATCH_BYTES`), one current export
transition (`<= MAX_BATCH_BYTES`), and the preview response allocated
capacity (`<= PREVIEW_RESPONSE_MAX_BYTES`). Operator state is
`<= MAX_OPERATOR_STATE_BYTES`.

### 6.1 Request field contracts

| Field | Contract |
| --- | --- |
| `plan` | Owned `LogicalPlan`; the complete E2 phase-1-shaped plan, not a prefix fragment |
| `target_node_id` | A non-nil `PlanNodeId` present in `plan.nodes`; must not be `Materialize` in C0 |
| `connection` | Injected `SourceConnection`; validated by the existing E2 preflight |
| `asset` | Injected `SourceAsset`; `asset.id` must be non-nil and bound to `Scan.source_asset_id` |
| `schema_override` | `None`, or a validated `LogicalSchema` used as the authorized source schema by E2 preflight |
| `context` | E2 `RequestContext`; carries cancellation and optional deadline |
| `batch_size` | `1..=ReadRequest::MAX_BATCH_SIZE` (65,536) |
| `row_limit` | `1..=PREVIEW_MAX_ROW_LIMIT`; `0` is invalid |
| `byte_limit` | `1..=PREVIEW_MAX_BYTE_LIMIT`; `0` is invalid |

Input scan caps are fixed public constants, not request fields:
`PREVIEW_MAX_SOURCE_ROWS_SCANNED = 100_000` and
`PREVIEW_MAX_SOURCE_BYTES_SCANNED = MAX_BATCH_BYTES` (64 MiB).

`PreviewRequest` must not contain `SnapshotStore`, identities, sampling
strategy, warnings, checkpoint, or a caller-held `PreparedPlan`.

### 6.2 Result field contracts

`PreviewResult` invariants are part of the public API. A successful result
must satisfy every invariant; violation is `EngineError::Internal` and the
in-memory response must be dropped before returning that error.

| Field | Invariant |
| --- | --- |
| `plan_fingerprint` | `request.plan.fingerprint()` computed by `stillflow-plan`; no second algorithm |
| `target_node_id` | Equals `request.target_node_id` |
| `schema` | The E2 working schema at the target boundary (section 11); validated |
| `batches` | Ordered from sequence 0; each is a validated `BatchEnvelope` for the target schema and bound `SourceAsset.id`; contiguous sequences; each envelope obeys `MAX_BATCH_ROWS` / `MAX_BATCH_BYTES` |
| `rows_returned` | `sum(envelope.row_count())`; `<= row_limit` |
| `bytes_returned` | `sum(envelope.byte_count())`; `<= byte_limit` |
| `source_rows_scanned` | Raw connector rows passed to target lowering; `<= PREVIEW_MAX_SOURCE_ROWS_SCANNED` |
| `source_bytes_scanned` | Sum of full `envelope.byte_count()` for every envelope from which any raw row was passed to lowering; `<= PREVIEW_MAX_SOURCE_BYTES_SCANNED` |
| `rows_truncated` | `true` iff lowering the observed scanned prefix produced more than `row_limit` target-output rows before terminal `None` or scan-cap stop; section 9.3 |
| `bytes_truncated` | `true` iff the byte cap removed at least one target-output row from the row-limited prefix of the observed scanned output; section 9.3 |
| `scan_truncated` | `true` iff input scanning stopped because of a source row/byte cap before terminal `None`; section 9.3 |
| `source_exhausted` | `true` iff the connector stream returned terminal `None`; it is observed, never derived from the other flags |

An empty source is a valid result: zero batches, zero rows, zero bytes,
zero scanned rows/bytes, all truncation fields false,
`source_exhausted = true`.

`source_exhausted` may be `true` together with `bytes_truncated = true`
when the byte cap excluded rows but the connector stream was then polled to
terminal `None`. It cannot be `true` together with `rows_truncated` or
`scan_truncated`.

Neither `PreviewRequest` nor `PreviewResult` implements `serde::Serialize`.
The batch payload remains the versioned `BatchEnvelope` boundary; no Polars
`DataFrame`, Polars `Series`, DuckDB connection, raw credential, or source
cell value may appear in either type.

## 7. Reuse of E2 semantics (no second engine)

E3 must call the same E2 preflight, typing, lowering, chunker, Arrow
envelope factory, and sanitized-error implementation used by `materialize`.
The only authorized E3 deltas are:

1. a private preview target argument threaded through the same preflight;
2. cutoff of the compiled step prefix at the target;
3. a bounded preview response accumulator, the four reporting flags, and
   the two source-scan counters;
4. preview-specific output and input-scan limits in section 8.

E3 must not re-implement or fork any of the following:

- plan validation, plan shape checks, iterative AST resource bounds,
  `MAX_PLAN_NODES`, `MAX_RULES_PER_NODE`, `MAX_EXPR_NODES`,
  `MAX_EXPR_DEPTH`, or compiled-plan byte limits;
- Scan binding, capability query, `push_projection` decision, or the four
  schema stages;
- Polars data-type mapping, Expr/Rule lowering, LUB conversion, nullability
  inference, cast/arithmetic error semantics, or paused operations;
- the deterministic execution chunker and predicted-column accounting;
- Arrow 59 metadata, `BatchEnvelopeFactory`, schema fingerprints, or
  `ColumnId` propagation;
- `EngineError` variants, category/retryability table, or
  `sanitized_summary()`.

## 8. Frozen resource ceilings

All limits are hard ceilings. Exceeding a maximum fails with the error named
in section 13. No TBD value is permitted.

| Resource | Ceiling | Default | Source |
| --- | --- | --- | --- |
| Preview rows returned | 10,000 | 1,000 | this contract |
| Preview bytes returned | 50 MiB | 8 MiB | this contract |
| Preview deadline | 30 s | 30 s | this contract |
| Preview concurrent requests | `MAX_ENGINE_CONCURRENT_RUNS` | same | shared E2 run gate |
| Source rows scanned per preview | 100,000 | same | this contract |
| Source bytes scanned per preview | 64 MiB | same | this contract |
| `batch_size` | 65,536 | 1,024 | E2 / `ReadRequest` |
| Input envelope rows | 65,536 | — | `stillflow-core` |
| Input envelope Arrow bytes | 64 MiB | — | `stillflow-core` |
| Current export transition | 64 MiB | — | Issue #46 §14.3 and section 10 |
| Preview response allocated capacity | `byte_limit` (max 50 MiB) | 8 MiB | this contract |
| Live columnar payloads | 3 | 3 | Issue #46 §14.1 |
| Operator state (including response metadata) | 5 MiB | — | Issue #46 §14.1 |
| Preview peak engine bytes | 183 MiB | — | section 10 |
| Plan nodes / rules / expr nodes / expr depth | 64 / 256 / 1,024 / 64 | — | Issue #46 §7 |
| Schema fields / depth / text | 4,096 / 64 / 1 MiB | — | #30 |

`row_limit == 0` or `byte_limit == 0` is `EngineError::InvalidPlan`.
`row_limit > PREVIEW_MAX_ROW_LIMIT`, `byte_limit > PREVIEW_MAX_BYTE_LIMIT`,
or `batch_size` outside `1..=65_536` is `EngineError::BoundExceeded` before
any connector I/O.

## 9. Target cutoff and execution semantics

### 9.1 Full-plan preflight

The preview plan is the complete E2 phase-1 plan from Issue #46 §9:

```text
Scan -> (Project | Filter | ApplyRules)* -> Materialize
```

Preflight validates the **whole** plan, including nodes downstream of the
target, exactly as `materialize` would. Downstream validation errors are
preview errors. Runtime execution stops at the target; validation is not
execution.

`Join` / `Union` anywhere in the plan, including downstream of the target,
must return `EngineError::UnsupportedOperator` before
`ConnectorRegistry::inspect`, before `read_batches`, and before any stream
poll.

### 9.2 Target binding

The shared preflight must insert the following target checks immediately
after E2 `linearize` and before `ConnectorRegistry::capabilities` /
`inspect` / schema resolution. No connector I/O occurs for a bad target.

| Condition | Error |
| --- | --- |
| `target_node_id` is nil or not present in `plan.nodes` | `InvalidPlan` |
| `target_node_id` is not on the unique `Scan -> Materialize` path | `InvalidPlan` |
| `target_node_id` is `Materialize` | `UnsupportedOperator` with kind `materialize` |
| `target_node_id` is `Join` / `Union` | `UnsupportedOperator` with that kind (already caught by the all-node scan) |
| `target_node_id` is `Scan` / `Project` / `Filter` / `ApplyRules` | valid; continue preflight |

A plan whose root is not `Materialize` is already an E2 `InvalidPlan`; E3
must not invent a fragment-plan preflight path.

### 9.3 Input scan bound and truncation law

Preview has two bounded stages: a raw input scan and target-output
accumulation. The complete target output of the underlying source is never
materialized and never used as the semantic object for flags. Flags are
operational: they describe only what this `preview` call observed.

**Input scan bounds**

- `source_rows_scanned` counts raw connector rows actually passed to target
  lowering. It must never exceed `PREVIEW_MAX_SOURCE_ROWS_SCANNED`
  (100,000).
- Before any raw row of a connector envelope is passed to lowering, the
  full `envelope.byte_count()` is charged to `source_bytes_scanned`. If the
  charge would exceed `PREVIEW_MAX_SOURCE_BYTES_SCANNED`
  (`MAX_BATCH_BYTES`, 64 MiB), that envelope is not consumed and scanning
  stops.
- Row cap: the implementation consumes the earliest raw-row prefix of the
  current envelope whose row count fits the remaining source-row budget.
  If only part of an envelope is consumed because the row budget reaches
  zero, scanning stops. The full envelope byte charge above is still
  recorded.
- At an exact source-row or source-byte boundary, the implementation must
  poll the stream one lookahead time: `Some` means `scan_truncated = true`
  and stop without consuming that envelope; terminal `None` means
  `source_exhausted = true` and `scan_truncated = false`.

**Target-output truncation**

Let `T` be the ordered target-output row sequence produced by lowering the
observed scanned raw-row prefix with the E2 prefix steps. Let `Q` be the
longest prefix of `T` with at most `row_limit` rows. Let `P` be the longest
prefix of `Q` with aggregate public `BatchEnvelope.byte_count()` at most
`byte_limit`.

- If the first row of `T` alone has `byte_count() > byte_limit`, `preview`
  returns `EngineError::BoundExceeded` and no `PreviewResult`.
- Otherwise `preview` returns exactly `P`, in target-output order.
- `rows_truncated = true` iff lowering the observed scanned prefix produced
  at least `row_limit + 1` target-output rows before terminal `None` or a
  scan-cap stop.
- `bytes_truncated = true` iff `bytes(Q) > byte_limit`.
- `scan_truncated = true` iff input scanning stopped because of a source
  row/byte cap before terminal `None`.
- `source_exhausted = true` iff the connector stream returned terminal
  `None`. It is never derived from the other flags.

**Deterministic flag-completion lookahead**

After `P` is closed by the byte cap, or when the row cap is reached exactly
at a lowering boundary, the implementation must continue scanning and
lowering discarded target-output rows only until the first of:

1. terminal `None` is observed → `source_exhausted = true`;
2. the lowering of the observed scan produces target-output row
   `row_limit + 1` → `rows_truncated = true`;
3. a source row/byte cap closes the scan → `scan_truncated = true`.

This lookahead counts **target-output rows**, never raw source rows. It is
bounded by the input scan caps, so a highly selective `Filter` /
`FilterRows` / `Scan.predicate` cannot turn Preview into a hidden full
import. Cancellation and deadline checks from section 13.2 remain active
during this lookahead.

`source_exhausted` may therefore be `true` together with
`bytes_truncated = true` when the stream was polled to terminal `None`
after byte truncation. It cannot be `true` together with `rows_truncated`
or `scan_truncated`.

This law defines the deterministic earliest prefix. Sampling, reservoir,
random selection, and reordering are forbidden.

### 9.4 Target node execution

The E2 compiled step sequence is split at the target. Steps whose originating
plan node is at or before the target execute; steps after the target do not.

| Target | Executes | Must not execute |
| --- | --- | --- |
| `Scan` | Scan binding and read; the synthetic `Project` when `push_projection` is false; `Scan.predicate` as the same in-engine `Filter` E2 uses | any later node |
| `Project` | Scan semantics plus that `Project` | any later node |
| `Filter` | Scan semantics plus every operator through that `Filter` | any later node |
| `ApplyRules` | Scan semantics plus every operator through that node; all rules in that node in listed order | any later node |

`Materialize` is never executed. Its `output_label` is still validated and
sanitized by the full E2 preflight.

### 9.5 Downstream rules are not executed

A supported downstream rule or operator after the target must have no effect
on rows or schema. It is still type-checked by full preflight. A downstream
`Rule::Validate` or `Rule::Deduplicate` is an E2 `UnsupportedRule` preflight
error before inspect, not a silently skipped node.

## 10. Execution path and memory model

For each successful `preview`:

```text
Connector BatchStream
  -> shared E2 preflight with target cutoff
  -> open read_batches exactly once
  -> bounded raw input scan (section 9.3)
  -> deterministic E2 execution chunker
  -> E2 Polars lowering of only the prefix steps
  -> E2 Arrow export and BatchEnvelopeFactory for the target schema
  -> bounded preview response accumulator
  -> PreviewResult returned in memory only
```

There is no canonical remainder for a future Snapshot and no
`SnapshotWriter`. The response accumulator replaces the remainder slot in
the three-payload memory law.

### 10.1 Public bytes vs allocated capacity

Public `bytes_returned` remains
`sum(envelope.byte_count())`. The memory proof must use a different, exact
quantity:

```text
response_allocated_capacity =
    sum of the capacities of every unique backing allocation owned by
    finalized response BatchEnvelope payloads
    + sum of current allocated capacities of every in-progress response
      builder buffer
```

Rules for `response_allocated_capacity`:

1. Each validity, offset, data, and child-array backing allocation is
   counted exactly once at its actual allocated capacity as recorded by the
   builder/allocator that produced it. `RecordBatch::get_array_memory_size()`
   is **not** sufficient evidence for this law.
2. `Vec<BatchEnvelope>` structs, envelope headers, `Arc<LogicalSchema>`,
   Arrow schema/metadata, compiled-plan objects, maps, and other
   non-columnar metadata are charged to `MAX_OPERATOR_STATE_BYTES` (5 MiB),
   not to the response payload.
3. Prefix slices must not retain an oversized parent buffer. When only a
   prefix of an exported Arrow chunk enters the response, the response must
   own a compacted allocation for exactly that prefix; a zero-copy slice
   that keeps the full parent buffer alive is forbidden. Any transient copy
   used for that compaction is part of `current_export_transition` below.
4. Freezing the in-progress builder into a `BatchEnvelope` is move/freeze:
   the backing allocations change owner, but are not duplicated or counted
   twice.

### 10.2 Coexistence and peak law

At every instant of one `preview` call the live columnar payloads are at
most three:

```text
connector envelope allocated capacity   <= MAX_BATCH_BYTES        (64 MiB)
current export transition               <= MAX_BATCH_BYTES        (64 MiB)
response allocated capacity             <= byte_limit             (<= 50 MiB)

operator state (including response metadata) <= MAX_OPERATOR_STATE_BYTES
peak                                          <= PREVIEW_PEAK_ENGINE_BYTES (183 MiB)
```

`current_export_transition(k)` is the E2 predictor bound for the current
execution chunk of `k` rows, including the remaining Polars columns, the
extracted canonical Arrow columns, and any compaction/export realloc
transients. It must satisfy the E2 `predict(k) <= MAX_BATCH_BYTES` law and
must not add an export copy outside that bound.

### 10.3 Response-aware prefix selection and builder realloc law

Let `k` be a candidate number of target-output rows to move from the
current export chunk into the response.

1. E2 chunker constraint:
   `predict(k) <= MAX_BATCH_BYTES`.
2. Response capacity constraint, checked **before** any builder reserve or
   reallocation:

   ```text
   other_response_capacity + old_capacity + requested_new_capacity <= byte_limit
   ```

   where `other_response_capacity` is `response_allocated_capacity`
   excluding the buffer being grown, `old_capacity` is that buffer's
   current allocated capacity, and `requested_new_capacity` is the exact
   capacity requested for the append. This law bounds the
   `old + new` realloc transient inside the response bound.
3. `predict_preview(k)` is the peak of the append transition:

   ```text
   predict_preview(k) =
       current_export_transition(k)
       + response_allocated_capacity_after(k)
       + builder_realloc_transient(k)
   ```

   where `response_allocated_capacity_after(k)` includes the new buffer
   capacities and `builder_realloc_transient(k)` is the capacity of old
   buffers still live during reallocation (zero when no reallocation
   occurs).

The implementation must choose the largest feasible `k` for the current
chunk such that both constraints hold. When only part of the response byte
budget remains, the response-aware prefix is smaller than the E2 chunker
prefix; this is `response_fit(k)` selection. If no `k >= 1` can satisfy the
response capacity constraint and the response is empty, fail with
`EngineError::BoundExceeded`. If the response is non-empty, stop with
`bytes_truncated = true` and do not append the non-fitting row.

The peak law then has this exact sum:

```text
connector envelope allocated capacity
+ current_export_transition(k)
+ response_allocated_capacity_after(k)
+ builder_realloc_transient(k)
+ operator state
<= PREVIEW_PEAK_ENGINE_BYTES
```

Because the response terms obey
`other_response_capacity + old_capacity + requested_new_capacity <=
byte_limit`, the response contribution including its realloc transient is
at most `byte_limit`; the other two columnar terms are each at most
`MAX_BATCH_BYTES`, and operator state is at most
`MAX_OPERATOR_STATE_BYTES`. Therefore 183 MiB remains the proven ceiling.

### 10.4 Chunking and accumulation

- Each consumed connector envelope is split by the E2 deterministic chunker
  before Arrow-to-Polars import. A full envelope whose `predict(n)` exceeds
  `MAX_BATCH_BYTES` must never be imported as one frame.
- A single row with E2 `predict(1) > MAX_BATCH_BYTES` is
  `EngineError::BoundExceeded` before Polars import, exactly as E2 T39.
- Transformed chunks are fed to the preview accumulator in order; the
  accumulator applies the response-aware prefix law of section 10.3.
- The accumulator uses E2 byte-accounting functions for capacity planning
  and `BatchEnvelope.byte_count()` only for the public returned-byte count.
- The accumulator must not materialize the full source and must not create a
  fourth `MAX_BATCH_BYTES`-class payload.

## 11. Schema and ColumnId propagation

The target schema is derived from the same E2 propagation pass that computes
the Materialize schema. E3 must not run a second propagation algorithm.

- Target `Scan`: result schema is the E2 Scan output schema, after
  `push_projection` handling and `Scan.predicate`.
- Target `Project` / `Filter`: the E2 working schema after that node.
- Target `ApplyRules`: the E2 working schema after the last rule in that
  node.
- Field order, display names, logical types, nullability, and `ColumnId`
  values are preserved exactly as E2 defines them. E3 generates no IDs.
- Envelopes use the canonical Arrow metadata from Issue #46 §12.4
  (`stillflow.schema.version`, `stillflow.schema.fingerprint`,
  `stillflow.schema.metadata`, `stillflow.column.id`,
  `stillflow.field.metadata`) and the bound `SourceAsset.id`.
- Envelope sequences start at 0 for every `preview` call and are contiguous.
- `PreviewResult.schema` must equal `batches[i].schema()` for every batch.

## 12. Concurrency

`preview` must use the **same** per-instance E2 run gate as `materialize`:

- `try_acquire_owned` only; waiting `acquire` is forbidden.
- The fifth concurrent request, counting `materialize` and `preview` calls
  together on one `ExecutionEngine`, returns `EngineError::Busy`
  immediately with `inspect` count 0 and `read_batches` poll count 0.
- No new semaphore, queue, task spawn set, or admission-control path is
  authorized.
- A dry-run public `preflight` still does not acquire the run gate; it has
  no preview target and remains the E2 materialize preflight.

## 13. Read-only, cancellation, deadline, and errors

### 13.1 Read-only guarantees

`preview` must not:

- construct `SnapshotDraft`, `SnapshotWriter`, or call
  `SnapshotStore`/`begin_snapshot`/`append`/`commit`;
- write Parquet partitions or SQLite rows;
- generate `Uuid`, `DateTime`, plan node IDs, column IDs, lineage, or
  quality scores;
- publish events, progress records, manifests, or partial result envelopes;
- retain credentials, paths, SQL, or source cell values in errors or Debug.

The only I/O is read-only connector inspect and `read_batches`.

### 13.2 Deadline and cancellation

`preview` must observe cancellation and deadline:

1. at entry, before run-gate acquisition and before inspect;
2. before opening `read_batches`;
3. on every connector stream poll, including scan and flag-completion
   lookahead polls (the existing context-attached stream);
4. before lowering each consumed connector envelope and each discarded
   target-output lookahead chunk;
5. before returning success.

If `context.deadline()` is `None`, `preview` applies
`PREVIEW_DEFAULT_DEADLINE` from entry. A caller deadline more than
`PREVIEW_MAX_DEADLINE` in the future is rejected with
`EngineError::BoundExceeded` in step 3 of section 14, before run-gate
acquisition and before connector I/O. A locally valid fifth concurrent
request therefore reaches the run gate and receives `Busy` with zero
connector calls.

A cancelled or timed-out preview returns `EngineError::Cancelled` or
`EngineError::Timeout` and never returns a partial `PreviewResult`.

### 13.3 Error mapping

E3 adds no `EngineError` variants. The complete Issue #46 §16 table remains
authoritative. Preview-specific failures use:

| Failure | Variant | Category | Retryable |
| --- | --- | --- | --- |
| Invalid target / zero limits / bad plan shape | `InvalidPlan` | `InvalidConfiguration` | false |
| Target `Materialize` | `UnsupportedOperator` | `UnsupportedCapability` | false |
| Limit above maximum / deadline too long / single response row over byte cap | `BoundExceeded` | `InvalidData` | false |
| Fifth concurrent request | `Busy` | `RateLimited` | true |
| Cancellation / timeout | `Cancelled` / `Timeout` | `Cancelled` / `Timeout` | false / true |

All errors use E2 `sanitized_summary()`. `EngineError` is not `Serialize`.

### 13.4 Sentinel sanitization

The sanitization sentinel is the UTF-8 string
`STILLFLOW_SENTINEL_CELL_VALUE_9f3c2a`. It must appear in a failing fixture
cell and must not appear in `EngineError` Display/Debug,
`sanitized_summary()` JSON, event metadata wrapping the summary, or any
Preview debug surface.

## 14. Request execution order

`preview` must execute in this order. A later step is not reached after an
earlier failure.

1. Clone `request.context`. If no deadline, set
   `now + PREVIEW_DEFAULT_DEADLINE`. Call `ensure_active()`.
2. Validate `batch_size`, `row_limit`, and `byte_limit` locally.
3. Reject `remaining() > PREVIEW_MAX_DEADLINE` as `BoundExceeded`.
4. `try_acquire_owned` on the shared E2 run gate. Failure is `Busy` with
   zero connector calls.
5. Call the shared E2 preflight with the private preview target. This is the
   only preflight pass and must not accept a caller `PreparedPlan`.
6. Compute the target step prefix and target schema from the same
   `PreparedPlan`.
7. Open `ConnectorRegistry::read_batches` exactly once and attach the E2
   context wrapper.
8. Run the bounded raw input scan from section 9.3, then chunk, lower only
   the prefix, and accumulate the bounded response. Continue the flag-
   completion lookahead exactly as long as section 9.3 requires; it counts
   target-output rows and stops at terminal `None`, target-output row
   `row_limit + 1`, or a source scan cap.
9. Record `source_rows_scanned` / `source_bytes_scanned` and the four
   observed flags. `source_exhausted` is true only when terminal `None` was
   actually observed.
10. Drop the stream and the permit before returning. Return the fully
   constructed `PreviewResult`; never return a partially filled buffer.

## 15. Connector call accounting

- `ConnectorRegistry::inspect` is called exactly once iff
  `schema_override` is `None` and all earlier checks pass. It is zero for
  Join/Union, invalid target, capability failure, plan-shape failure, or any
  earlier error.
- `ConnectorRegistry::read_batches` is opened exactly once for a request
  that reaches execution, and zero times otherwise.
- Stream poll count is deterministic for a fixed mock stream:
  - no truncation / source exhaustion: one poll per consumed source
    envelope plus one terminal `None` poll;
  - target-output row truncation: poll and lower only until target-output
    row `row_limit + 1` is observed, terminal `None`, or a scan cap closes;
    raw source rows are not used as the truncation test;
  - byte truncation: returned rows close at the first non-fitting
    target-output row; flag-completion lookahead then continues under the
    same target-output rule and the input scan caps;
  - scan-cap boundary: exactly one lookahead poll distinguishes
    `scan_truncated = true` (`Some`) from `source_exhausted = true`
    (terminal `None`).
- P10 freezes exact counts for named fixtures.

## 16. Implementation checklist (later E3 runtime PR only)

1. Add `PreviewRequest`, `PreviewResult`, preview constants, and
   `ExecutionEngine::preview` as specified in section 6.
2. Thread a private preview target through the shared preflight at the exact
   stage in section 9.2.
3. Reuse the E2 chunker, lowerer, Arrow export, `BatchEnvelopeFactory`, and
   error sanitizer without forking them.
4. Implement the bounded preview response accumulator and truncation flags.
5. Add the P01–P15 tests and mock-call counters.
6. Run repository checks. Do not include Dependabot diffs.

## 17. Acceptance matrix

Every criterion must be objectively automatable by the later E3 runtime PR.

### P01 — Target cutoff for every supported node

For one deterministic fixture and one plan
`Scan -> Project -> Filter -> ApplyRules -> Materialize`, run four previews
with targets `Scan`, `Project`, `Filter`, and `ApplyRules`. Each result must
equal the E2 logical output of the prefix through that node (schema, rows,
field order, `ColumnId`s) and must not contain the effects of later nodes.
No preview may execute `Materialize`.

### P02 — Downstream rules do not run

Plan `Scan -> Project -> ApplyRules(Rename/Derive/ReplaceLiteral) -> ...`.
Target `Project`. Assert returned schema and rows are the Project output;
the downstream rename/derive/replace effects are absent. The same plan
preflights successfully, proving validation is not execution.

### P03 — Invalid or missing target node

Targets: nil `PlanNodeId`, a UUID not in `plan.nodes`, a node id from a
different valid plan, and `Materialize`. Assert typed `InvalidPlan` or
`UnsupportedOperator` as section 9.2 requires, with mock `inspect` count 0,
`read_batches` open count 0, and stream poll count 0.

### P04 — Join/Union rejected before connector inspect

For a valid E2 prefix followed by `Join` or `Union`, and for a target equal
to a `Join`/`Union` node, assert `EngineError::UnsupportedOperator`,
`category() == UnsupportedCapability`, `retryable() == false`, inspect count
0, read poll count 0. Downstream Join/Union is rejected even when the target
is upstream of it.

### P05 — Target-output row/byte truncation and source exhaustion

1. Filter-aware row truncation: a `Filter` passes every third source row;
   the observed target output exceeds `row_limit`. Assert
   `rows_truncated = true`, `bytes_truncated = false`,
   `scan_truncated = false`, `source_exhausted = false`, and returned rows
   equal `row_limit` in target-output order.
2. Byte truncation with terminal source: total target output is below
   `row_limit`, but `bytes(Q) > byte_limit`; lookahead then polls the
   stream to terminal `None`. Assert the returned rows are the longest
   byte-fitting earliest prefix, `rows_truncated = false`,
   `bytes_truncated = true`, `scan_truncated = false`,
   `source_exhausted = true`.
3. Byte and row truncation: byte cap closes before `row_limit`, and
   lookahead observes target-output row `row_limit + 1` before any scan cap
   or terminal `None`. Assert `rows_truncated = true`,
   `bytes_truncated = true`, `scan_truncated = false`,
   `source_exhausted = false`.
4. Scan row cap: a source with more than 100,000 raw rows and a selective
   filter that has not produced `row_limit + 1` target rows when the raw
   row budget closes. Assert returned rows are all observed target-output
   rows, `scan_truncated = true`, `rows_truncated = false`,
   `source_exhausted = false`, and `source_rows_scanned = 100_000`.
5. Scan byte cap: the first consumed envelope charges the full 64 MiB
   source-byte budget and the lookahead poll observes another envelope.
   Assert `scan_truncated = true`, `source_exhausted = false`, and the
   lookahead envelope is not consumed or charged.

Assert aggregate `rows_returned` / `bytes_returned` equal their envelope
sums and obey both output caps. Assert `source_rows_scanned` /
`source_bytes_scanned` obey both input scan caps. Assert every returned
row set is the deterministic earliest prefix, not a sample.

### P06 — Single row exceeds byte cap

Set `byte_limit` below the first transformed target row's `BatchEnvelope`
byte count. Assert `EngineError::BoundExceeded`, no `PreviewResult`, no
Snapshot/Storage call, and no partial batch publication.

### P07 — Repeated execution is identical

Run the same request twice against an immutable fixture. Compare
`plan_fingerprint`, `schema`, logical rows, envelope sequences,
`row_count`/`byte_count` per envelope, `source_rows_scanned`,
`source_bytes_scanned`, and all four truncation/source flags. They must be
equal. Source grep shows no `Uuid::new_v4` / `Utc::now` on the preview
path.

### P08 — Cancellation and deadline

1. Cancel before `read_batches` and cancel during lowering: `Cancelled`, no
   partial result.
2. Cancel during the flag-completion lookahead of section 9.3: `Cancelled`,
   no partial result.
3. Expired deadline before returning, including during scan/lookahead:
   `Timeout`, no partial result.
4. No deadline: default 30 s is applied.
5. Deadline farther than 30 s in the future: `BoundExceeded` before
   inspect/read.

### P09 — Fifth concurrent request is immediately Busy

Hold four in-flight requests on one `ExecutionEngine` using a mix of
`materialize` and `preview`. A fifth `preview` returns `Busy`,
`category() == RateLimited`, `retryable() == true`, mock inspect count 0 and
read poll count 0. Releasing one permit admits the next request without a
second gate.

### P10 — Connector inspect/read call counts

With mock counters and fixed stream partitions, assert exact counts for:

- Join/Union/invalid-target plans: inspect 0, read open 0, poll 0.
- Valid preview without `schema_override`: inspect 1, read open 1.
- Valid preview with `schema_override`: inspect 0, read open 1.
- No-truncation three-envelope stream: poll 4 (three envelopes + terminal
  `None`), `source_exhausted = true`.
- Filter row truncation where source and target rows are not equal: one
  envelope with 40 source rows, filter keeps every third row,
  `row_limit = 10`; poll 1, `rows_truncated = true` because target-output
  row 11 is observed inside that envelope.
- Byte truncation with terminal source: poll through terminal `None`;
  `source_exhausted = true` even though `bytes_truncated = true`.
- Scan-cap boundary: a stream whose consumed envelopes exactly reach a
  source cap requires one additional lookahead poll; `Some` sets
  `scan_truncated = true`, terminal `None` sets `source_exhausted = true`.

### P11 — SnapshotWriter and storage publication zero calls

The preview runtime path must not construct `SnapshotDraft`, call
`SnapshotStore::begin_snapshot`, `SnapshotWriter::append`,
`SnapshotWriter::commit`, or invoke any other Snapshot publication entry
point. Ordinary preview response-accumulator append operations are allowed
and must not be matched by this check.

To make the zero-call assertion executable without a public API change, E3
may add `#[cfg(test)]`-only private storage-call counters in the storage
crate or engine test support. They must not alter production behavior or
public API. P11 invokes `preview` and asserts those counters remain 0 and
that no manifest, event, or partition is published.

### P12 — Schema and ColumnId propagation

For targets `Scan`, `Project`, and `ApplyRules`, assert
`PreviewResult.schema` equals the corresponding E2 propagated working
schema, including field order, display names, nullability, and `ColumnId`s.
Every returned envelope carries the canonical Arrow metadata and target
schema fingerprint. No ColumnId is generated or renamed.

### P13 — Secret sentinel never enters errors, events, or Debug

A fixture cell contains
`STILLFLOW_SENTINEL_CELL_VALUE_9f3c2a` and causes a preview `CastFailure`
or `Arithmetic` at the target. Assert the sentinel is absent from
`EngineError` Display/Debug, `serde_json::to_string(sanitized_summary())`,
and event metadata wrapping the summary. `EngineError` itself does not
implement Serialize.

### P14 — Preview response allocated-capacity and working-set proof

Using the E2 live-payload counter, a counting allocator, and exact buffer
capacity records:

- live columnar payload count is `<= 3` while a connector envelope is split
  into at least two E2 chunks and finalized response batches remain live;
- `response_allocated_capacity <= byte_limit` at every instant, including
  the old-buffer/new-buffer realloc transient;
- every builder reserve/realloc obeys
  `other_response_capacity + old_capacity + requested_new_capacity <=
  byte_limit` before allocation;
- `current_export_transition(k) <= MAX_BATCH_BYTES`, connector envelope
  capacity `<= MAX_BATCH_BYTES`, and operator state `<= 5 MiB`;
- the peak sum of section 10.3 is `<= PREVIEW_PEAK_ENGINE_BYTES`
  (183 MiB);
- a prefix response envelope owns a compacted allocation for exactly its
  prefix and does not retain an oversized parent buffer;
- `Vec<BatchEnvelope>` / schema / metadata overhead is charged to operator
  state, not to response capacity;
- source grep forbids collecting the full connector stream and forbids a
  fourth `MAX_BATCH_BYTES`-class payload.

### P15 — Docs-only delivery and frozen SHA

This docs PR diff touches only Issue #50, the architecture roadmap, and the
development workflow. No Rust, Cargo, lockfile, CI, or frontend file is
modified. Every numeric limit in section 8 is explicit and nonzero. The PR
is draft with Request changes and binds the contract commit SHA. Work stops
for architecture approval; no E3 runtime code is added in this PR.

## 18. Stop conditions

Stop and return to contract review if implementation needs:

- a public type or field not named in section 6;
- a dependency not already authorized by Issue #46 §6.3;
- a second preview executor, preflight, typing, lowering, chunker, or error
  taxonomy;
- a waiting or queued preview admission path;
- full-source materialization or a fourth `MAX_BATCH_BYTES`-class payload;
- generated IDs/timestamps or any Snapshot/Storage publication;
- Join/Union/Materialize execution;
- non-prefix sampling or reordering;
- a Polars type in the public API;
- a message that includes a cell value, credential, path, SQL, or
  `output_label`;
- serializing `EngineError` or emitting events that carry it.

## 19. Known risks

- E2 runtime is not yet merged. E3-C0-R1 binds to Issue #46 R3 semantics as
  merged in `main@4b65204`; any later approved E2 semantic revision
  requires an explicit E3 contract revision, not silent adoption.
- Full-plan preflight means downstream validation can fail a preview whose
  target is upstream. This is intentional reuse of one preflight and is
  covered by P02/P04.
- The target-output flag lookahead may scan and lower up to the fixed input
  scan caps after the visible prefix is closed. It is bounded by
  100,000 source rows and 64 MiB source bytes, and must not retain those
  rows.
- `source_exhausted` is operational. A scan-cap boundary requires one
  lookahead poll; if the next item is `Some`, the result reports
  `scan_truncated = true` and does not consume that envelope.
- Preview response bytes use public `BatchEnvelope.byte_count()`, while the
  memory proof uses exact allocated capacity. The two numbers differ by
  design; both are frozen here.
- A single transformed row that exceeds the preview byte cap is a
  `BoundExceeded`, not an empty successful result.
- The 183 MiB peak is valid only because the response realloc transient is
  included in the response bound by the pre-allocation law in section 10.3.
