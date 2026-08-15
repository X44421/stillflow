# Issue #48 E2-R1 Contract Addendum

> Status: **Proposed** (not approved)
> Risk: High
> Issue: #48
> Parent contract: Issue #46 revision R3, approved at
> `32f1c53d9903f66aeaca1c2676c0b81abfb2a702`, merged in PR #47
> Implementation PR: #49 (draft)
> Branch: `agent/issue-048-deterministic-engine-executor`
> Last updated: 2026-08-15
> Review: Request changes on `15536eca`. This document is the proposed
> delta. It is not frozen. Do not treat any line as “already approved.”

This addendum does **not** replace the frozen R3 contract. Architecture
must approve a SHA of this file before those deltas are considered
authorized. Runtime on #49 may implement only what this proposal names,
and must keep the PR draft.

Do not open or continue the E1 contract branch. Do not expand remaining
operators (T01–T36 / T38 / T40 / T42) in this revision.

## 1. Objective

Record every contract change required to correct `15536eca` without
silently editing frozen R3:

1. retract unapproved dependency edits;
2. make remainder a true append/freeze builder;
3. make `predict(k)` per-rule and nested-aware;
4. make T37 / T39 / T41 / T43 / T44 independent and objective;
5. close type-checking gaps, or pause unimplemented paths;
6. eliminate production panics and context gaps.

## 2. What R3 actually authorized

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
`regex`. Labeling those as “already-approved workspace crates” in
`15536eca` was a contract violation.

The frozen Issue #46 file on this branch must match R3 plus the two
nits. Proposed deltas live only here.

## 3. Proposed dependency deltas

These are proposed, not approved:

| Change | Why | E2-R1 behavior until this SHA is approved |
| --- | --- | --- |
| Remove `dtype-u32` from the Polars feature list | Polars 0.46 does not expose that feature; `UInt32` is always available | Omit the feature so the crate compiles. This is a factual correction that still needs review. |
| Do **not** add `regex` | `Expr::Contains` / `contains_literal` needs it | Preflight `TypeError`: Contains is paused |
| Do **not** add `arrow-select` | Concat allocated a fourth columnar payload | Remainder uses `arrow-array` builders already covered by R3 |
| Do **not** add `arrow-cast` | Utf8View → Utf8 can use `StringBuilder` | Engine-owned conversion, no extra crate |

`serde_json` remains a **dev-dependency only**. Production
`stillflow-engine` must not depend on `serde` / `serde_json`.

If architecture later authorizes `regex` so Contains can run, that is a
new approved SHA of this addendum, not a silent Cargo.toml edit.

## 4. Remainder: append/freeze (normative)

R3 §14.1 already forbids a fourth `MAX_BATCH_BYTES`-class copy and
requires remainder → output to be **move/freeze**. `15536eca` violated
that by `arrow_select::concat` while existing remainder arrays and the
incoming batch were both live, and by dropping the Polars tracker slot
before the exported batch was counted.

Live columnar payloads remain at most three:

```text
connector envelope
  + (complete Polars working set XOR incoming canonical chunk)
  + remainder builder
```

Rules:

1. Import a predicted slice into Polars. That working set is one payload.
2. Export to a canonical Arrow batch, then **drop the Polars frame**.
   The exported batch becomes the incoming canonical payload (same slot,
   not a fourth slot).
3. Incoming remains counted until every row has been appended into the
   remainder builder or the incoming handle is dropped.
4. Remainder is a **builder** whose unfinished buffers are one payload.
   Append copies values into those buffers. It must not allocate a
   finished concatenated `RecordBatch` while previous remainder arrays
   and incoming arrays are both live.
5. Freeze/move finishes the builder into one output envelope (move of
   builder buffers). `SnapshotWriter::append` may borrow that envelope.
   After `append` returns, drop it. The builder is empty.
6. If the next incoming prefix does not fit, freeze/move the current
   remainder first, then retry against an empty builder.
7. Storage Parquet encode remains excluded from `MAX_ENGINE_PEAK_BYTES`.

Forbidden: `concat` (or equivalent) that yields `envelope + incoming +
old remainder + new remainder`.

## 5. Predictor

`predict(k)` is the maximum over **every node and every rule**, not over
a collapsed `ApplyRules` step:

```text
predict_step(k) = live_before(k) + temporary_allocation(k) + live_after(k)
predict(k)      = max over steps of predict_step(k)
```

After each rule, `live_before` for the next rule is that rule’s
`live_after`.

ReplaceLiteral, FillNull, and Cast **must recompute** `live_after` from
the updated `PredictedColumn` table (type, `nullable`,
`max_value_bytes`). Returning the previous `live_before` as `live_after`
is incorrect when width or nullability changes.

Source slices bill the logical range `[offset, offset + k)`:

- validity `(k + 7) / 8` if a bitmap is required;
- fixed-width `k * slot`;
- Utf8/Binary `utf8_physical_bytes(k, offsets[i+k] - offsets[i])`
  (or equivalent per-value sum when offsets are unavailable);
- List: the same rules on the child array over the sliced element
  range;
- Struct: the sum of children over the same row range.

A zero-copy Arrow slice must not bill unused parent bytes a second time.
Those parent bytes stay attributed to the live connector envelope.

## 6. Operator-state accounting

Utf8 (and other) literals in the plan count toward
`MAX_OPERATOR_STATE_BYTES` (5 MiB). A Derive literal of
`MAX_BATCH_BYTES + 1` must fail `BoundExceeded` in preflight before
inspect/import. T39 must not smuggle a 64 MiB literal through as “just
predicted expansion.”

## 7. Typed compilation and pauses

Preflight must type-check expressions against the working schema:

- `Filter` / `FilterRows` predicates are Boolean;
- comparisons use R3 comparable / ordered rules and LUB;
- arithmetic uses R3 LUB and result types;
- `And` / `Or` require Boolean operands;
- `Contains` requires Utf8 operands (when not paused);
- unknown columns remain `UnknownColumn`.

E2-R1 **pauses** these paths with preflight `TypeError` (inspect count 0,
read count 0) until a later approved addendum implements them correctly:

| Path | Reason |
| --- | --- |
| `Expr::Contains` | polars `regex` is not an approved R3 dependency |
| `Add` / `Subtract` / `Multiply` / `Divide` / `Modulo` / `Negate` | checked overflow and toward-zero integer division are not yet implemented at row granularity |
| `Timestamp { unit: Second, .. }` in schema or expr | Polars 0.46 has no second unit; silent `Second → Milliseconds` is forbidden |
| `Date32` / `Timestamp` → `Utf8` | already paused in R3 |
| `List` / `Struct` in a transforming plan | remainder builders and nested execution are not in this slice |

`Timestamp` millisecond / microsecond / nanosecond remain authorized.
Passthrough of nested types without rules may be paused with the same
`TypeError` rather than silently corrupting remainder accounting.

Authorized in this slice when type-checked: `Scan`, `Project`, `Filter`
(Boolean predicates without paused ops), `ApplyRules` of Rename, Cast
(non-paused), Trim, ReplaceLiteral, FillNull, DropColumn, DeriveColumn,
FilterRows, and `Materialize`.

## 8. Panic, context, and `batch_size`

Production engine paths must not use `unreachable!`, `unwrap`, or
`expect`. `sanitized_summary` fallback must use nested `try_new` on
static secret-free literals, then `ConnectorError::sanitized_summary`
of a typed internal failure — never `unreachable!`.

Validate / Deduplicate schema arms return `UnsupportedRule`.

After `inspect` returns, call `context.ensure_active()` again before
using the inspected schema.

`ExecutionRequest.batch_size` must be in
`ReadRequest::MIN_BATCH_SIZE..=ReadRequest::MAX_BATCH_SIZE` (`1..=65_536`)
before `read_batches` and before constructing the remainder pack limit.

## 9. Tests (independent)

Each ID is its own `#[test]` / `#[tokio::test]`. A shared helper is
allowed; merging T37+T41+T44 into one assertion function is not.

| ID | Evidence |
| --- | --- |
| T37 | 2 KiB UTF-8 Derive over 65,536 input rows; snapshot `row_count == 65_536`; chunker `k < 65_536`; live Polars working set `<= MAX_BATCH_BYTES`; peak engine bytes `<= MAX_ENGINE_PEAK_BYTES`; live payloads `<= 3` |
| T39 | FFI import counter is 0; `BoundExceeded`; no snapshot. Cover (a) a literal that exceeds 5 MiB operator state, and (b) `predict(1) > MAX_BATCH_BYTES` with a literal that fits in 5 MiB |
| T41 | one envelope split into `>= 2` chunks while remainder from the first chunk is live together with envelope + Polars; live payloads `<= 3`; snapshot row_count matches input |
| T43 | chosen `k` satisfies `predict(k) <= MAX_BATCH_BYTES < predict(k+1)`, using the view/offset/validity formula, or `BoundExceeded` with import count 0 when `predict(1)` exceeds the cap |
| T44 | a **phased test allocator** (not a handwritten peak field) records (a) Polars, (b) remainder builder/freeze, (c) storage append. `(a)+(b)+5 MiB <= MAX_ENGINE_PEAK_BYTES`; (c) is recorded and excluded from the engine ceiling |

The phased allocator is a `#[global_allocator]` used only in
`stillflow-engine` lib tests, with a thread-local phase. Idle/fixture
allocations are not attributed to (a)/(b)/(c).

T39’s FFI import counter increments only inside the engine Arrow→Polars
import path.

## 10. Non-goals for E2-R1

- Remaining operators and tests T01–T36, T38, T40, T42, T45 beyond
  keeping the existing Date32→Utf8 pause.
- Join / Union execution, Validate / Deduplicate, Preview HTTP, DuckDB,
  SQLx, API, frontend, Dependabot.
- New public types beyond R3 §7.
- A new contract branch.

## 11. Stop conditions

Stop and return to contract review if implementation needs:

- `arrow-select`, `arrow-cast`, polars `regex`, or any crate absent from
  R3 §6.3 except the proposed `dtype-u32` omission;
- a fourth live columnar payload;
- `concat` of remainder + incoming into a new finished batch while both
  sources remain live;
- `Second → Milliseconds` without this addendum’s pause;
- production `unreachable!` / `unwrap` / `expect`;
- expanding paused Contains / arithmetic / nested types without a new
  approved SHA.

## 12. Approval binding

Architecture approval of E2-R1 binds the git SHA that contains this
file as **Proposed** at the time of the review comment. Implementation
commits on #49 after that SHA must match this text. Until approval,
#49 stays draft.
