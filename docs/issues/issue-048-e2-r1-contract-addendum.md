# Issue #48 E2-R1A Contract Addendum

> Status: **Proposed** (not approved)
> Risk: High
> Issue: #48
> Parent contract: Issue #46 revision R3, approved at
> `32f1c53d9903f66aeaca1c2676c0b81abfb2a702`, merged in PR #47
> Implementation PR: #49 (draft)
> Branch: `agent/issue-048-deterministic-engine-executor`
> Last updated: 2026-08-15
> Review: Request changes on `5808ffadc8c9f807c4138af2f597425813fb51db`. This
> document is the proposed delta revision E2-R1A-R1 (docs-only). It is not
> frozen. Do not treat any line as “already approved.”

This addendum does **not** replace the frozen R3 contract. Architecture must
approve a SHA of this file before those deltas are considered authorized.
Runtime on #49 may implement only what this proposal names, and must keep the
PR draft.

Do not open or continue the E1 contract branch. Do not expand remaining
operators (T01–T36 / T38 / T40 / T42) in this revision.

---

## 1. Objective

Record every contract change and explicit specification required to resolve the
P0 and P1 review blockers raised on `5808ffadc8c9f807c4138af2f597425813fb51db`
without silently editing frozen R3:

1. **Polars → Arrow export transition & unified coexistence model**: Formally
   amend R3 §6.2.1 to authorize engine-owned typed column conversion in place of
   C ABI export. Unify the export transition lifecycle so that the coexistence
   of remaining Polars series, extracted Arrow arrays, builder peak, and
   reallocation transients is bounded by `MAX_BATCH_BYTES` (64 MiB) and
   incorporated into `predict(k)`.
2. **Remainder builder capacity, pre-allocation checks, & timezone retention**:
   Define exact capacity tracking across all sink buffer types, mandate dynamic
   builder initialization (prohibiting 4,096-column `pack_limit` bulk
   over-allocation), require pre-allocation capacity checks (`other_live +
   old_capacity + requested_new <= 64 MiB`) before reserving memory, mandate
   the 64 MiB check in `hold_incoming`, and preserve timestamp timezone metadata.
3. **Phased test allocator & 197 MiB memory law proof (T44)**: Explicitly break
   down the 197 MiB ceiling into connector envelope (<= 64 MiB), transition
   working set (<= 64 MiB), remainder allocated capacity (<= 64 MiB), and
   operator state (<= 5 MiB). Specify multi-threaded allocator tracking,
   `realloc` transient peak accounting (`old + new`), allocation-origin tracking
   for cross-phase deallocations, test isolation harnesses, and active RAII
   wrapping of `writer.append`.
4. **Strict AST validation ordering, LUB casts, & type boundaries**: Mandate
   that iterative AST validation (node count and depth) is the absolute first
   traversal in preflight, require explicit `.strict_cast` to LUB in lowering,
   require typed-null derivation matching the declared target type, fix Float →
   Utf8 prediction at 32 bytes, and enforce strict Binary cast boundaries.
5. **Workspace build & test profile governance**: Explicitly specify the
   `backend/.cargo/config.toml` (`jobs = 2`) and `backend/Cargo.toml`
   (`[profile.test]` `debug = 0`, `codegen-units = 4`) workspace configuration
   changes as proposed deltas.
6. **Sanitized error fallback category**: Clarify fallback summary construction
   to permit `ConnectorError::internal(static_msg).sanitized_summary()` to
   guarantee `ErrorCategory::Internal` without synthetic capability drift.
7. **Comprehensive R1A acceptance criteria**: Define dedicated acceptance tests
   covering all new addendum specifications.

---

## 2. What R3 Actually Authorized

[#47 approval](https://github.com/X44421/stillflow/pull/47#issuecomment-5294143308)
and [#48](https://github.com/X44421/stillflow/issues/48) authorized only:

- one `PredictedColumn.nullable` field (drop duplicate `nullability`);
- rename `MAX_LIVE_COLUMNAR_BUFFERS` → `MAX_LIVE_COLUMNAR_PAYLOADS`.

R3 §6.3 at `32f1c53` authorizes engine dependencies:

- `stillflow-plan`, `stillflow-storage`;
- `tokio`, `tokio-util`, `futures`, `thiserror`, `uuid`, `chrono`;
- `arrow-array` (ffi), `arrow-schema` (ffi), `arrow-data = "59"`;
- `polars-arrow = "0.46"`;
- `polars` 0.46 with **exactly**
  `lazy`, `strings`, `dtype-u8`, `dtype-u16`, `dtype-u32`, `dtype-i8`,
  `dtype-i16`, `dtype-date`, `dtype-datetime`, `dtype-struct`.

R3 does **not** authorize `arrow-select`, `arrow-cast`, or polars
`regex`. Labeling those as “already-approved workspace crates” was a contract
violation.

The frozen Issue #46 file on this branch must match R3 plus the two
nits. Proposed deltas live only here.

---

## 3. Proposed Dependency & Build Configuration Deltas

These are proposed, not approved:

| Change | Location | Why | E2-R1A behavior until this SHA is approved |
| --- | --- | --- | --- |
| Remove `dtype-u32` from Polars features | `backend/crates/stillflow-engine/Cargo.toml` | Polars 0.46 does not expose that feature; `UInt32` is always built-in | Omit feature so crate compiles. Factual correction requiring review. |
| Do **not** add `regex` | `backend/crates/stillflow-engine/Cargo.toml` | `Expr::Contains` / `contains_literal` needs it | Preflight `TypeError`: `Contains` is paused. |
| Do **not** add `arrow-select` | `backend/crates/stillflow-engine/Cargo.toml` | `concat` allocates a fourth columnar payload | Remainder uses incremental builder append. |
| Do **not** add `arrow-cast` | `backend/crates/stillflow-engine/Cargo.toml` | Utf8View → Utf8 handled by engine conversion | Engine-owned conversion, no extra crate. |
| Add `arrow-buffer = "59"` | `backend/crates/stillflow-engine/Cargo.toml` | Remainder Utf8/Binary freeze moves exact `Vec` buffers into `StringArray`/`BinaryArray` | Direct dep on Arrow 59 substrate already pulled by `arrow-array`/`arrow-data`. |
| Set `jobs = 2` | `backend/.cargo/config.toml` | Windows RAM exhaustion during parallel rustc debug builds & CI exit 143 | Explicitly capped compiler jobs. |
| Set `[profile.test]` `debug = 0`, `codegen-units = 4` | `backend/Cargo.toml` | Polars/Arrow test linking consumes multi-GB peak RAM with full DWARF | Lean test profile avoids OOM/hangs. |

`serde_json` remains a **dev-dependency only**. Production
`stillflow-engine` must not depend on `serde` / `serde_json`.

---

## 4. Polars → Arrow Export Transition (§6.2.1 Amendment)

### 4.1 Specification Amendment

Frozen R3 §6.2.1 assumed C ABI export (`export_array_to_c` / `export_field_to_c`)
from Polars into Arrow 59. In Polars 0.46, C ABI export produces `Utf8View` arrays
and non-canonical structures that do not match canonical Arrow 59 representations
without secondary conversions.

E2-R1A formally amends §6.2.1 as follows:

1. **Export Mechanism**: Polars → Arrow 59 export is executed via engine-owned
   stateless typed column conversion (`dataframe_to_record_batch`).
2. **Unified Export Transition Coexistence Model**:
   During the export phase, columns are extracted sequentially from the Polars
   `DataFrame` into canonical Arrow arrays. The memory occupied by this step
   is the **export transition working set**:
   ```text
   export_transition_bytes = max_i(
       remaining_polars_capacity(i)
       + finished_arrow_capacity(i)
       + current_builder_peak(i)
       + realloc_transient(i)
   )
   ```
   where `i` indexes over each column extracted from `0..num_columns`.
   The export transition working set is bounded by `MAX_BATCH_BYTES` (64 MiB).
3. **Predictor Law for Export Transition**:
   The chunker prediction formula must explicitly incorporate the export
   transition peak:
   ```text
   predict_step(k) = live_before(k) + temporary_allocation(k) + live_after(k)
   predict_export(k) = live_after(k) [remaining Polars] + live_after(k) [extracted Arrow batch] + realloc_transient(k)
   predict_chunk(k) = max(max_steps(predict_step(k)), predict_export(k))
   ```
   `predict_chunk(k)` must satisfy `predict_chunk(k) <= MAX_BATCH_BYTES` (64 MiB).
4. **Deferred Materialization for Derived Literals**:
   For constant literal derived columns (e.g. wide UTF-8 literals), Polars
   evaluates shape and nullability without allocating replicated cell bytes in
   memory. The canonical Arrow array is constructed directly during export using
   exact offset and value buffers, avoiding duplicate buffer materialization in
   both Polars and Arrow.

---

## 5. Remainder: Builder Capacity, Pre-Allocation, & Timezone Retention

### 5.1 Append / Freeze Model & Three-Payload Memory Law

Frozen R3 §14.1 and E2-R1 §4 forbid a fourth `MAX_BATCH_BYTES`-class payload.
Simultaneously live columnar payloads remain at most three:

```text
connector envelope (<= 64 MiB)
  + export transition working set (Polars working set & incoming chunk extraction) (<= 64 MiB)
  + remainder builder allocated capacity (<= 64 MiB)
```

The sum of all three columnar payloads plus bounded operator state (<= 5 MiB)
must not exceed `MAX_ENGINE_PEAK_BYTES` (197 MiB).

### 5.2 Capacity Accounting vs Allocated Overhead

1. **Allocated Capacity Measurement**:
   `remainder_bytes()` must measure the **total allocated capacity** of all
   underlying builder buffers across all columns, not merely the logical row
   count multiplied by slot size:
   - Fixed-width primitives: `allocated_capacity * slot_bytes`
   - Boolean: `values_capacity / 8 + null_capacity / 8`
   - VariableBytes (Utf8 / Binary): `offsets_capacity * 4 + values_capacity + validity_capacity / 8`
   - Null: `allocated_capacity / 8`
2. **Prohibition of Bulk Pre-allocation on Initialization**:
   `CanonicalRebatcher::new` must **not** pre-allocate `pack_limit` rows across
   all schema columns (which on 4,096 columns would allocate gigabytes of unused
   memory). Sinks must initialize with zero or minimal capacity and grow
   dynamically as rows are appended.
3. **Pre-Allocation Capacity Check Rule**:
   Before allocating or reserving memory for **any** builder buffer (primitive
   values, boolean bitmaps, offsets, variable-width values, or validity
   bitmaps), the sink must verify:
   ```text
   other_live_capacity + old_capacity + requested_new_capacity <= MAX_BATCH_BYTES
   ```
   - If this sum exceeds `MAX_BATCH_BYTES` (64 MiB) and the remainder contains
     unflushed rows (`remainder.rows > 0`), the rebatcher must immediately
     freeze/move the current remainder into an output envelope, publish it,
     reset the builder to empty, and retry appending against the empty builder.
   - If the remainder is already empty and `requested_new_capacity` still exceeds
     `MAX_BATCH_BYTES`, fail immediately with `BoundExceeded("a single row exceeds MAX_BATCH_BYTES")`.
   - Post-allocation capacity checks are strictly forbidden; capacity must be
     checked *before* allocation occurs.
4. **Incoming Chunk Guard**:
   `MemoryTracker::hold_incoming(bytes)` must explicitly assert
   `bytes <= MAX_BATCH_BYTES`.
5. **Timestamp Timezone Preservation**:
   `ColumnSink::Timestamp` must retain both `TimeUnit` (Millisecond,
   Microsecond, Nanosecond) and `Option<String>` timezone metadata. Freezing the
   sink must construct a `TimestampArray` preserving the exact schema
   `timezone`. Dropping timezone metadata during freeze is a contract violation.

---

## 6. Phased Test Allocator & 197 MiB Memory Law Proof (T44)

T44 must provide rigorous, non-simulated runtime evidence of isolated memory
peaks and compliance with the 197 MiB engine memory law:

1. **Memory Law Equation**:
   ```text
   peak_live_engine_bytes = envelope_bytes + transition_phase_peak + remainder_phase_peak + operator_state_bytes
   peak_live_engine_bytes <= MAX_ENGINE_PEAK_BYTES = 197 MiB
   envelope_bytes <= MAX_BATCH_BYTES = 64 MiB
   transition_phase_peak <= MAX_BATCH_BYTES = 64 MiB
   remainder_phase_peak <= MAX_BATCH_BYTES = 64 MiB
   operator_state_bytes <= MAX_OPERATOR_STATE_BYTES = 5 MiB
   ```
   Each columnar component must be independently verified to be `<= 64 MiB`.
2. **Multi-Threaded Global Tracking**:
   The test global allocator must track all allocations across all threads
   spawned during execution, including Rayon/Polars worker threads and Tokio
   runtime worker threads.
3. **Transient Realloc Accounting**:
   During `realloc(ptr, layout, new_size)`, the system allocator temporarily
   holds both the old memory block and the new memory block. The test allocator
   must account for the transient peak `layout.size() + new_size` before
   decrementing the old layout size.
4. **Allocation-Origin Attribution for Cross-Phase Deallocations**:
   When memory allocated in phase X (e.g. `AllocatorPhase::Polars`) is freed
   while the active phase is Y (e.g. `AllocatorPhase::Remainder` or `Idle`), the
   deallocation must be attributed to the allocation's origin phase (or tracked
   via global live tracking), rather than deducting from phase Y. Phase live
   counters must not underflow.
5. **Test Isolation**:
   To prevent multi-threaded test pollution across parallel test executions,
   tests executing phased allocator assertions must run with test isolation
   (e.g., `--test-threads=1`, exclusive test mutex locks, or dedicated test
   binary execution).
6. **Active Storage Append RAII Enclosure**:
   `AllocatorPhase::StorageAppend` must actively wrap the call to
   `SnapshotWriter::append(&envelope)` (actual Parquet encoding and storage
   I/O). The test allocator must record storage append peak memory `(c)` and
   verify that `(c)` is excluded from the engine peak budget. Dummy simulated
   allocations are strictly forbidden.

---

## 7. Iterative AST Validation, Typed Compilation, & Type Boundaries

### 7.1 Strict AST Validation Ordering in Preflight

Iterative AST validation (`validate_expr_iterative` using an explicit stack to
verify `node_count <= MAX_EXPR_NODES` and `depth <= MAX_EXPR_DEPTH`) must be the
**absolute first traversal** performed on any plan expression. It must execute
before:

1. `compiled_plan_bytes(plan)`
2. `reject_paused_plan_exprs(plan)`
3. Type inference and nullability inference (`type_check_expr`, `infer_nullability`)
4. Expression lowering and cast checking.

Expressions exceeding node count or depth limits must fail fast with
`BoundExceeded` / `InvalidPlan` before any recursive visitor or type evaluator
is invoked.

### 7.2 Explicit LUB Strict-Casting in Lowering

When evaluating binary comparisons (`Equal`, `NotEqual`, `LessThan`,
`LessThanOrEqual`, `GreaterThan`, `GreaterThanOrEqual`), numeric arithmetic, or
`Coalesce`:

- If operand types differ but possess a valid Least Upper Bound `T`, lowering
  must insert explicit `.strict_cast(polars_data_type(T))` on the mismatched
  operand(s) prior to applying the Polars operator.
- Relying on implicit Polars type coercion is strictly forbidden.

### 7.3 Typed Null Derivation

`Rule::DeriveColumn` with `Expr::Literal(ScalarValue::Null)` must construct a
typed null column matching the declared target `LogicalType` (e.g.,
`polars::prelude::Column::full_null(name, height, &polars_data_type(target_type))`),
**never** an untyped `DataType::Null`.

### 7.4 Float → Utf8 32-Byte Predictor Boundary

`Rule::Cast` or `Expr::Cast` from `Float32` or `Float64` to `LogicalType::Utf8`
must bill `MAX_FLOAT_UTF8_BYTES` = 32 bytes per row in `predict(k)` and in
`PredictedColumn.max_value_bytes`.

### 7.5 Binary Type Cast Boundaries

Explicit `Cast` to or from `LogicalType::Binary` is authorized strictly for
identity `Binary -> Binary`. Any cast from non-Binary to Binary, or from Binary
to non-Binary, must fail at preflight with `TypeError("cast to/from binary is not authorized")`.

---

## 8. Summary of Paused Execution Paths (E2-R1A)

E2-R1A continues to pause the following paths with preflight `TypeError`
(inspect count 0, read count 0):

| Path | Reason |
| --- | --- |
| `Expr::Contains` | Polars `regex` feature is not an approved R3 dependency |
| `Add` / `Subtract` / `Multiply` / `Divide` / `Modulo` / `Negate` | Checked overflow and toward-zero integer division semantics not implemented at row granularity |
| `Timestamp { unit: Second, .. }` | Polars 0.46 has no second unit; silent scaling is forbidden |
| `Date32` / `Timestamp` → `Utf8` | Paused per R3 pending provable formatting width bounds |
| `List` / `Struct` transforming execution | Nested builders and nested execution postponed |
| Non-Binary ↔ Binary casts | Disallowed type conversions |

---

## 9. Panic Freedom & Sanitized Error Fallback

1. **Zero Panic Invariant**:
   Production paths must contain zero `unwrap()`, `expect()`, or `unreachable!()`.
   `Rule::Validate` and `Rule::Deduplicate` arms must return
   `EngineError::UnsupportedRule`.
2. **Sanitized Error Fallback**:
   `SanitizedErrorSummary.message` is a private field whose public constructor
   is `try_new` returning a `Result`. Fallback paths must construct sanitized
   summaries via:
   ```rust
   SanitizedErrorSummary::try_new(ErrorCategory::Internal, false, "internal error")
       .unwrap_or_else(|_| ConnectorError::internal("internal error").sanitized_summary())
   ```
   guaranteeing `ErrorCategory::Internal` and `retryable: false` without panic or
   capability drift.

---

## 10. Test Matrix & Acceptance Evidence

| ID | Focus | Acceptance Evidence |
| --- | --- | --- |
| T37 | Execution Chunker | 2 KiB UTF-8 Derive over 65,536 input rows; snapshot `row_count == 65_536`; chunker `k < 65_536`; live Polars working set `<= MAX_BATCH_BYTES`; peak engine bytes `<= MAX_ENGINE_PEAK_BYTES`; live payloads `<= 3`. |
| T39 | Operator State & Expansion | FFI import counter is 0; `BoundExceeded`; no snapshot. Covers (a) literal exceeding 5 MiB operator state, and (b) `predict(1) > MAX_BATCH_BYTES` with literal fitting in 5 MiB. |
| T41 | Remainder Coexistence | One envelope split into `>= 2` chunks while remainder from the first chunk is live together with envelope + Polars; live payloads `<= 3`; snapshot row_count matches input. |
| T43 | Exact Cap Boundary | Chosen `k` satisfies `predict(k) <= MAX_BATCH_BYTES < predict(k+1)`, using view/offset/validity overhead formula. |
| T44 | Phased Memory Law Proof | Real phased allocator records (a) Polars / export transition, (b) remainder builder / freeze, (c) storage append wrapping `writer.append`. Asserts `(a) <= 64 MiB`, `(b) <= 64 MiB`, `envelope <= 64 MiB`, `envelope + (a) + (b) + 5 MiB <= 197 MiB`, and `(c)` excluded from engine peak. |
| T45 | Paused Cast | Cast `Date32` or `Timestamp` to `Utf8` fails preflight with `TypeError`. |
| T46 | Near-64 MiB Export Transition | Export transition working set respects `max_i(remaining_polars + finished_arrow + builder + realloc) <= 64 MiB` without unbounded materialization. |
| T47 | 4,096-Column Allocation | Constructing schema with 4,096 fixed-width columns starts with minimal builder capacity and does not pre-allocate `pack_limit` rows per column. |
| T48 | Timestamp Timezone Retention | Timestamps with non-empty timezone retain timezone metadata through rebatching and snapshot emission without drift. |
| T49 | Iterative AST Guard | Ultra-deep AST (`depth > MAX_EXPR_DEPTH`) fails fast in preflight before `inspect`, `read`, or recursive type passes. |
| T50 | LUB Strict-Casting | Lowering of mixed-type comparisons (e.g. `Int32` vs `Int64`) and `Coalesce` emits explicit strict-casts to LUB without implicit coercion. |
| T51 | Typed Null Derivation | `DeriveColumn` with `Literal(Null)` produces a column matching the declared target `LogicalType` (not `DataType::Null`). |
| T52 | Float → Utf8 Prediction Bound | `Cast` from `Float64` to `Utf8` bills 32 bytes per row in `predict(k)` and `PredictedColumn`. |
| T53 | Binary Cast Rejection | Casts between `Binary` and non-`Binary` types fail at preflight with `TypeError`. |
| T54 | Fallback Error Sanitization | Fallback sanitized error summary always resolves to `ErrorCategory::Internal` and `retryable: false`. |

---

## 11. Non-Goals for E2-R1A

- Implementing remaining operators T01–T36, T38, T40, T42.
- Unpausing arithmetic, regex `Contains`, or nested `List`/`Struct` transforms.
- Join / Union execution, Validate / Deduplicate runtime.
- Frontend, API, DuckDB, or SQLx integration.

---

## 12. Approval Binding

Architecture approval of E2-R1A binds the git SHA containing this document as
**Proposed**. Implementation commits on #49 following approval must strictly
conform to this text. Until approval, PR #49 remains in draft.
