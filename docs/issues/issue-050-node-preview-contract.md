# Issue #50 Implementation Contract: node-level Preview (E3-C0)

> Status: Frozen for architecture review (not approved)
> Revision: C0
> Risk: High
> Issue: #50
> Parent contract: Issue #46 revision R3, merged at
> `32f1c53d9903f66aeaca1c2676c0b81abfb2a702` in PR #47
> Authorized base: `main@4b65204cfdb69c73389fba77cf4fd9715e94cba`
> Branch: `agent/issue-050-node-preview-contract`
> Last updated: 2026-08-15
> Review: Keep the PR draft and Request changes. Architecture approval binds
> exactly one commit SHA of this file. Runtime implementation starts only
> after that approval.

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
- Deterministic earliest-prefix truncation and the three reporting fields.
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
pub const PREVIEW_DEFAULT_DEADLINE: Duration = Duration::from_secs(30);
pub const PREVIEW_MAX_DEADLINE: Duration = Duration::from_secs(30);
pub const PREVIEW_MAX_CONCURRENT_REQUESTS: usize = 4;
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
    pub rows_truncated: bool,
    pub bytes_truncated: bool,
    pub source_exhausted: bool,
}

impl ExecutionEngine {
    pub async fn preview(
        &self,
        request: PreviewRequest,
    ) -> Result<PreviewResult, EngineError>;
}
```

`PREVIEW_MAX_CONCURRENT_REQUESTS` is a documented alias of the E2 run-gate
capacity `MAX_ENGINE_CONCURRENT_RUNS` (4). E3 must not create a second
semaphore or any other admission-control primitive.

`PREVIEW_PEAK_ENGINE_BYTES` is a hard ceiling. The three columnar payloads
are one connector envelope (`<= MAX_BATCH_BYTES`), one complete Polars
working set (`<= MAX_BATCH_BYTES`), and the preview response buffer
(`<= PREVIEW_RESPONSE_MAX_BYTES`). Operator state is
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
| `rows_truncated` | `true` iff the source contains more than `row_limit` rows; section 9.3 |
| `bytes_truncated` | `true` iff the byte cap removed at least one row from the row-limited prefix; section 9.3 |
| `source_exhausted` | `true` iff both truncation fields are false; all source rows were returned |

An empty source is a valid result: zero batches, zero rows, zero bytes,
`rows_truncated = false`, `bytes_truncated = false`, `source_exhausted = true`.

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
3. a bounded preview response accumulator and the three reporting fields;
4. preview-specific limits in section 8.

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
| Preview response buffer | `byte_limit` (max 50 MiB) | 8 MiB | this contract |
| Preview deadline | 30 s | 30 s | this contract |
| Preview concurrent requests | 4 | 4 | shared E2 run gate |
| `batch_size` | 65,536 | 1,024 | E2 / `ReadRequest` |
| Input envelope rows | 65,536 | — | `stillflow-core` |
| Input envelope Arrow bytes | 64 MiB | — | `stillflow-core` |
| Complete Polars working set | 64 MiB | — | Issue #46 §14.1 |
| Live columnar payloads | 3 | 3 | Issue #46 §14.1 |
| Operator state | 5 MiB | — | Issue #46 §14.1 |
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

### 9.3 Truncation law

Let `S` be the ordered target output row sequence for the complete source,
and let `Q` be the longest prefix of `S` with at most `row_limit` rows. Let
`P` be the longest prefix of `Q` with aggregate `BatchEnvelope.byte_count()`
at most `byte_limit`.

- If the first row of `S` alone has `byte_count() > byte_limit`, `preview`
  returns `EngineError::BoundExceeded` and no `PreviewResult`.
- Otherwise `preview` returns exactly `P`, in source order.
- `rows_truncated = true` iff `S` has more than `row_limit` rows.
- `bytes_truncated = true` iff `bytes(Q) > byte_limit`.
- `source_exhausted = true` iff `rows_truncated == false` and
  `bytes_truncated == false`.

This law defines the deterministic earliest prefix. Sampling, reservoir,
random selection, and reordering are forbidden.

To compute `rows_truncated` without unbounded work, the implementation may
continue polling the connector stream after the returned prefix is closed by
the byte cap, but only to count source rows until it has observed
`row_limit + 1` source rows or the stream returns `None`. Such lookahead
must not lower rows through Polars, must retain at most one connector
envelope at a time, and must not grow the response buffer.

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
  -> deterministic E2 execution chunker
  -> E2 Polars lowering of only the prefix steps
  -> E2 Arrow export and BatchEnvelopeFactory for the target schema
  -> bounded preview response accumulator
  -> PreviewResult returned in memory only
```

There is no canonical remainder for a future Snapshot and no
`SnapshotWriter`. The response accumulator replaces the remainder slot in
the three-payload memory law.

### 10.1 Live payloads

At every instant of one `preview` call:

```text
live payloads <= 3

connector envelope            <= MAX_BATCH_BYTES  (64 MiB)
complete Polars working set   <= MAX_BATCH_BYTES  (64 MiB)
preview response buffer       <= byte_limit       (<= 50 MiB)

operator state                <= MAX_OPERATOR_STATE_BYTES (5 MiB)
peak                         <= PREVIEW_PEAK_ENGINE_BYTES (183 MiB)
```

The preview response buffer is the union of every finalized
`BatchEnvelope` in the result plus the in-progress response builder. At
every append/freeze boundary:

```text
bytes_returned + in_progress_builder_bytes <= byte_limit
```

Freezing the in-progress builder into a `BatchEnvelope` is move/freeze, not
a second copy. The E2 predictor bound (`predict(k) <= MAX_BATCH_BYTES`)
continues to include transformation and export-transition temporaries; E3
must not add an export copy outside that bound.

### 10.2 Chunking and accumulation

- Each connector envelope is split by the E2 deterministic chunker before
  Arrow-to-Polars import. A full envelope whose `predict(n)` exceeds
  `MAX_BATCH_BYTES` must never be imported as one frame.
- A single row with E2 `predict(1) > MAX_BATCH_BYTES` is
  `EngineError::BoundExceeded` before Polars import, exactly as E2 T39.
- Transformed chunks are fed to the preview accumulator in order.
- The accumulator builds target-schema `BatchEnvelope`s whose total rows and
  bytes obey the truncation law. It must use the E2 byte-accounting
  functions for capacity checks and `BatchEnvelope.byte_count()` for the
  public returned-byte count.
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
3. on every connector stream poll (the existing context-attached stream);
4. before lowering each connector envelope;
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
8. Stream, chunk, lower only the prefix, accumulate the bounded response.
9. Drop the stream and the permit before returning. Return the fully
   constructed `PreviewResult`; never return a partially filled buffer.

## 15. Connector call accounting

- `ConnectorRegistry::inspect` is called exactly once iff
  `schema_override` is `None` and all earlier checks pass. It is zero for
  Join/Union, invalid target, capability failure, plan-shape failure, or any
  earlier error.
- `ConnectorRegistry::read_batches` is opened exactly once for a request
  that reaches execution, and zero times otherwise.
- Stream poll count is deterministic for a fixed mock stream:
  - no truncation: one poll per source envelope plus one terminal `None`
    poll;
  - row truncation: polls until cumulative source rows first exceed
    `row_limit` (this may be inside an already-polled envelope);
  - byte truncation: returned rows close at the first non-fitting row; the
    stream may then be polled only for the bounded `rows_truncated`
    lookahead in section 9.3.
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

### P05 — Row and byte double truncation

Three fixtures:

1. more than `row_limit` small rows: returned rows equal `row_limit`;
   `rows_truncated = true`; `bytes_truncated = false`;
   `source_exhausted = false`.
2. fewer than `row_limit` rows but bytes over `byte_limit`: returned rows
   are the longest byte-fitting earliest prefix; `rows_truncated = false`;
   `bytes_truncated = true`; `source_exhausted = false`.
3. more than `row_limit` rows and bytes over `byte_limit` before
   `row_limit`: both `rows_truncated` and `bytes_truncated` are true when
   the lookahead observes row `row_limit + 1`.

Assert aggregate `rows_returned`/`bytes_returned` equal their envelope sums
and obey both caps. Assert the returned rows are the earliest prefix, not a
sample.

### P06 — Single row exceeds byte cap

Set `byte_limit` below the first transformed row's `BatchEnvelope` byte
count. Assert `EngineError::BoundExceeded`, no `PreviewResult`, no
Snapshot/Storage call, and no partial batch publication.

### P07 — Repeated execution is identical

Run the same request twice against an immutable fixture. Compare
`plan_fingerprint`, `schema`, logical rows, envelope sequences,
`row_count`/`byte_count` per envelope, and all three truncation fields. They
must be equal. Source grep shows no `Uuid::new_v4` / `Utc::now` on the
preview path.

### P08 — Cancellation and deadline

1. Cancel before `read_batches` and cancel during lowering: `Cancelled`, no
   partial result.
2. Expired deadline before returning: `Timeout`, no partial result.
3. No deadline: default 30 s is applied.
4. Deadline farther than 30 s in the future: `BoundExceeded` before
   inspect/read.

### P09 — Fifth concurrent request is immediately Busy

Hold four in-flight requests on one `ExecutionEngine` using a mix of
`materialize` and `preview`. A fifth `preview` returns `Busy`,
`category() == RateLimited`, `retryable() == true`, mock inspect count 0 and
read poll count 0. Releasing one permit admits the next request without a
second gate.

### P10 — Connector inspect/read call counts

With mock counters, assert exact counts for:

- Join/Union/invalid-target plans: inspect 0, read open 0, poll 0.
- Valid preview without `schema_override`: inspect 1, read open 1.
- Valid preview with `schema_override`: inspect 0, read open 1.
- No-truncation three-envelope stream: poll 4 (three envelopes + terminal
  `None`).
- Row truncation exactly at an envelope boundary: one extra lookahead poll
  decides `rows_truncated`/`source_exhausted`.
- Byte truncation lookahead stops at `row_limit + 1` observed source rows or
  terminal `None`.

### P11 — SnapshotWriter zero calls

Source grep of the E3 preview runtime path must contain no
`SnapshotWriter`, `SnapshotDraft`, `SnapshotStore`, `begin_snapshot`,
`append`, or `commit` call. A preview test uses an instrumented storage
sentinel/event recorder and asserts no manifest, no event, and no partition
is published. `PreviewRequest` contains no `store` field.

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

### P14 — Preview response and working-set memory proof

Using the E2 live-payload counter and capacity checks:

- live payload count is `<= 3` while a connector envelope is split into at
  least two E2 chunks and a non-empty preview response buffer is retained;
- connector envelope `<= 64 MiB`, current Polars working set `<= 64 MiB`,
  response buffer `<= request.byte_limit` (and `<= 50 MiB`);
- `bytes_returned + in_progress_builder_bytes <= byte_limit` before every
  freeze;
- peak engine-owned bytes obey `PREVIEW_PEAK_ENGINE_BYTES = 183 MiB`;
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

- E2 is not yet merged. E3-C0 binds to Issue #46 R3 semantics as merged in
  `main@4b65204`; any later approved E2 semantic revision requires an
  explicit E3 contract revision, not silent adoption.
- Full-plan preflight means downstream validation can fail a preview whose
  target is upstream. This is intentional reuse of one preflight and is
  covered by P02/P04.
- The `rows_truncated` lookahead may poll a bounded number of extra
  connector envelopes after byte truncation. It is capped at
  `row_limit + 1` observed source rows and must not retain them.
- Preview response bytes use public `BatchEnvelope.byte_count()`, while the
  chunker uses E2 predicted physical bytes. The two numbers are different by
  design; both are frozen here.
- A single transformed row that exceeds the preview byte cap is a
  `BoundExceeded`, not an empty successful result.
