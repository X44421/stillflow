# Issue #48 E2-R1A Contract Addendum

> Status: **Proposed** (not approved)
> Risk: High
> Issue: #48
> Parent contract: Issue #46 revision R3, approved at
> `32f1c53d9903f66aeaca1c2676c0b81abfb2a702`, merged in PR #47
> Implementation PR: #49 (draft)
> Branch: `agent/issue-048-deterministic-engine-executor`
> Last updated: 2026-08-15
> Review: Request changes on `c3de55a`. This document is the proposed
> delta revision E2-R1A (docs-only). It is not frozen. Do not treat any line
> as “already approved.”

This addendum does **not** replace the frozen R3 contract. Architecture
must approve a SHA of this file before those deltas are considered
authorized. Runtime on #49 may implement only what this proposal names,
and must keep the PR draft.

Do not open or continue the E1 contract branch. Do not expand remaining
operators (T01–T36 / T38 / T40 / T42) in this revision.

---

## 1. Objective

Record every contract change and explicit specification required to resolve
the P0 and P1 review blockers raised on `c3de55a` without silently editing
frozen R3:

1. **Polars → Arrow export transition & memory bounds**: Formally amend R3
   §6.2.1 to authorize engine-owned typed column conversion in place of C ABI
   export, bound the export transition peak memory, and incorporate export copy
   accounting into `predict(k)`.
2. **Remainder builder capacity, transients, & timezone preservation**: Define
   capacity tracking (`builder_allocated_capacity_bytes`), dynamic allocation
   growth without unmetered pack_limit over-allocation, transient reallocation
   bounds, mandatory 64 MiB payload check in `hold_incoming`, and preservation
   of timezone metadata in timestamp sinks.
3. **Phased test allocator & objective memory verification**: Specify real
   process-wide / multi-threaded allocation accounting across stages, replace
   counter simulation with actual RAII phase guards spanning `writer.append`,
   and preserve correct `realloc` diff accounting.
4. **Iterative typed compilation, LUB casts, & type boundaries**: Define
   iterative AST validation preceding type checking, mandatory explicit LUB
   strict-casting for binary operations and Coalesce, typed-null derivation
   preserving the target physical logical type, 32-byte prediction for Float → Utf8
   casts, and complete Binary type cast boundaries.
5. **Workspace build & test profile governance**: Explicitly specify the
   `backend/.cargo/config.toml` (`jobs = 2`) and `backend/Cargo.toml`
   (`[profile.test]` `debug = 0`, `codegen-units = 4`) workspace configuration
   changes as proposed deltas.
6. **Sanitized error fallback category**: Fix fallback summary category
   resolution to guarantee `Internal` rather than forwarding unmatched
   unsupported-capability structures.

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
   stateless typed column extraction (`dataframe_to_record_batch`).
2. **Column Extraction Lifecycle**:
   - Columns are extracted individually from the `DataFrame`.
   - To avoid holding the entire `DataFrame` simultaneously with all newly
     allocated Arrow arrays, columns must be extracted sequentially or by moving
     series out of the frame.
   - For constant literal derived columns (e.g. wide UTF-8 literals), the engine
     defers allocation: Polars evaluates metadata/shape, and the engine
     constructs the canonical `StringArray` directly with exact offset buffers,
     avoiding duplicated materialization in both Polars and Arrow.
3. **Export Memory Bound & Predictor Law**:
   During export transition, both the Polars frame (or remaining columns) and the
   newly extracted Arrow arrays coexist temporarily in memory before the Polars
   frame is completely dropped.
   - The export copy memory is bounded by `live_after(k)`.
   - The chunker prediction formula must explicitly account for export transition:
     ```text
     predict_step(k) = live_before(k) + temporary_allocation(k) + live_after(k)
     predict_export(k) = live_after(k) [Polars frame] + live_after(k) [Arrow batch]
     predict_chunk(k) = max(max_steps(predict_step(k)), predict_export(k))
     ```
   - `predict_chunk(k)` must be `<= MAX_BATCH_BYTES` (64 MiB).

---

## 5. Remainder: Builder Capacity, Reallocation, & Timezone Retention

### 5.1 Append / Freeze Model & Memory Law

Frozen R3 §14.1 and E2-R1 §4 forbid a fourth `MAX_BATCH_BYTES`-class payload.
Live columnar payloads remain at most three:

```text
connector envelope
  + (Polars working set XOR incoming canonical chunk)
  + remainder builder
```

### 5.2 Capacity Accounting vs Allocated Overhead

1. **Allocated Capacity Accounting**:
   - `remainder_bytes()` must measure the **allocated capacity** of all builder
     buffers (`capacity * slot_bytes` for fixed-width; `capacity_bytes` of values
     + offsets + validity for variable-width), not merely `rows * slot_bytes`.
   - Over-allocation at construction (e.g. pre-allocating `pack_limit` rows across
     4,096 columns) is strictly forbidden. Builders must initialize with zero or
     minimal capacity and grow dynamically.
2. **Transient Reallocation Bounds**:
   - When variable-width builders (`VariableBytes`) grow, reallocation must use
     bounded geometric or exact growth such that total allocated capacity plus
     transient reallocation buffer does not exceed `MAX_BATCH_BYTES`.
   - `hold_incoming(bytes)` must explicitly assert `bytes <= MAX_BATCH_BYTES`.
3. **Timestamp Timezone Preservation**:
   - `ColumnSink::Timestamp` must store both `TimeUnit` (Millisecond,
     Microsecond, Nanosecond) and `Option<String>` timezone.
   - Freezing a timestamp sink must produce a `TimestampArray` retaining the
     exact schema `timezone` metadata. Dropping timezone during freeze is a contract
     violation.
4. **Single-Row Overflow**:
   - If `k == 0` when incoming is pushed into an empty remainder builder, the
     single row exceeds `MAX_BATCH_BYTES`. Fail immediately with
     `BoundExceeded`.

---

## 6. Phased Test Allocator & Objective Verification (T44)

T44 must provide rigorous, non-simulated runtime evidence of isolated memory
peaks:

1. **Real RAII Scopes Across Worker Threads**:
   - The test global allocator must track allocations across all threads spawned
     during execution (e.g., Rayon/Polars worker pools and Tokio runtime threads).
   - Global atomic phase tracking or thread-inherited context must ensure that
     background Polars threads are attributed to `AllocatorPhase::Polars`.
2. **Accurate Realloc Accounting**:
   - `realloc(ptr, layout, new_size)` must record net memory change atomically
     without negative under-counting before system reallocation.
3. **Enclosing Actual Storage Append**:
   - `AllocatorPhase::StorageAppend` must actively wrap the invocation of
     `writer.append(&envelope)` (Parquet encoding and I/O), not a simulated
     dummy allocation.
   - The phase must switch to `StorageAppend` before calling `writer.append` and
     revert to `Idle` or the enclosing phase only after `writer.append` returns.
4. **Assertion Law**:
   - Peak Polars phase allocation `(a)` + Peak Remainder phase allocation `(b)` +
     `MAX_OPERATOR_STATE_BYTES` (5 MiB) must be `<= MAX_ENGINE_PEAK_BYTES`
     (197 MiB).
   - Storage append peak `(c)` is recorded and verified to be excluded from the
     engine peak budget.

---

## 7. Typed Compilation, LUB Casts, & Type Boundaries

### 7.1 Iterative AST Validation Preceding Type Checking

- Prior to recursive type evaluation, `validate_expr` must verify node count
  (`<= MAX_EXPR_NODES`) and nesting depth (`<= MAX_EXPR_DEPTH`) iteratively
  using an explicit stack.
- If limits are exceeded, return `BoundExceeded` / `InvalidPlan` immediately
  without triggering deep stack recursion.

### 7.2 LUB Strict-Casting in Lowering

- When evaluating binary comparisons (`Equal`, `NotEqual`, `Lt`, `Le`, `Gt`,
  `Ge`), arithmetic, or `Coalesce`:
  - If operand types differ but possess a valid Least Upper Bound `T`, lowering
    must emit explicit `.strict_cast(polars_data_type(T))` on the mismatched
    operand(s) before applying the operator.
  - Relying on implicit Polars coercion is forbidden.

### 7.3 Typed Null Derivation

- `Rule::DeriveColumn` with `Expr::Literal(ScalarValue::Null)` must construct a
  column matching the declared target `LogicalType` (e.g., full-null `Int32`,
  `Utf8`, etc.), **not** `DataType::Null`.

### 7.4 Prediction for Float → Utf8 Cast

- `Rule::Cast` or `Expr::Cast` from `Float32` or `Float64` to `LogicalType::Utf8`
  must bill `MAX_FLOAT_UTF8_BYTES` = 32 bytes per row in `predict(k)` and
  `PredictedColumn.max_value_bytes`.

### 7.5 Binary Type Cast Boundaries

- Explicit `Cast` to or from `LogicalType::Binary` is authorized only for
  identity `Binary -> Binary`.
- Any cast from non-Binary to Binary, or Binary to non-Binary, must fail at
  preflight with `TypeError("cast to/from binary is not authorized")`.

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

1. **Zero Panic Paths**:
   - Production code must contain zero `unwrap()`, `expect()`, or `unreachable!()`.
   - `Rule::Validate` and `Rule::Deduplicate` arms must return
     `EngineError::UnsupportedRule`.
2. **Sanitized Error Fallback**:
   - `fallback_summary()` must directly construct `SanitizedErrorSummary` with
     `ErrorCategory::Internal`, `retryable: false`, and a static sanitized
     message. It must not delegate to a synthetic connector error that could
     yield `ErrorCategory::UnsupportedCapability`.

---

## 10. Test Matrix & Acceptance Evidence

| ID | Focus | Acceptance Evidence |
| --- | --- | --- |
| T37 | Execution Chunker | 2 KiB UTF-8 Derive over 65,536 input rows; snapshot `row_count == 65_536`; chunker `k < 65_536`; live Polars working set `<= MAX_BATCH_BYTES`; peak engine bytes `<= MAX_ENGINE_PEAK_BYTES`; live payloads `<= 3`. |
| T39 | Operator State & Expansion | FFI import counter is 0; `BoundExceeded`; no snapshot. Covers (a) literal exceeding 5 MiB operator state, and (b) `predict(1) > MAX_BATCH_BYTES` with literal fitting in 5 MiB. |
| T41 | Remainder Coexistence | One envelope split into `>= 2` chunks while remainder from the first chunk is live together with envelope + Polars; live payloads `<= 3`; snapshot row_count matches input. |
| T43 | Exact Cap Boundary | Chosen `k` satisfies `predict(k) <= MAX_BATCH_BYTES < predict(k+1)`, using view/offset/validity overhead formula. |
| T44 | Phased Memory Allocation | Real phased allocator records (a) Polars working set, (b) remainder builder/freeze, (c) storage append wrapping `writer.append`. Asserts `(a)+(b)+5 MiB <= MAX_ENGINE_PEAK_BYTES` and `(c)` excluded. |
| T45 | Paused Cast | Cast `Date32` or `Timestamp` to `Utf8` fails preflight with `TypeError`. |

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
