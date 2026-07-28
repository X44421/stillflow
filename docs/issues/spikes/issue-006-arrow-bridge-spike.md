# Issue #6 — Arrow Bridge Spike Report

**Date:** 2026-07-28

**Branch:** `agent/mvp-006-local-tabular`

**Contract:** [`issue-006-implementation-contract.md`](../issue-006-implementation-contract.md) (FROZEN)

**Phase:** Spike Phase 1 — Option A only

**Status:** **Option A technical validation passed; production scheme NOT approved.**

---

## Contract deviation (BLOCKER for production)

```text
Contract deviation:
Option A requires unsafe Arrow FFI import and ABI conversion.
This conflicts with Frozen invariant #12 / Forbidden change #11.
Production implementation remains blocked pending Sol approval.
```

Frozen contract references:

- **Invariant #12:** No `unsafe` in project code for Arrow bridging.
- **Forbidden change #11:** Add `unsafe` bridging code.
- **Spike acceptance table:** “No `unsafe` in project code | Yes”.

Option A uses:

1. `mem::transmute` between `polars_arrow::ffi::ArrowArray` / `ArrowSchema` and arrow-rs `FFI_ArrowArray` / `FFI_ArrowSchema` (ABI layout assumption).
2. `arrow_array::ffi::from_ffi_and_data_type` — an **`unsafe` API**; caller must guarantee the export conforms to the [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html). See [arrow 59 `from_ffi_and_data_type` docs](https://docs.rs/arrow/latest/arrow/array/ffi/fn.from_ffi_and_data_type.html).

**Sol decision required (pick one):**

1. Approve a **strictly scoped** `unsafe` exception confined to `stillflow-connector-local-tabular/src/bridge/ffi.rs` with documented invariants; or
2. Reject Option A as incompatible with the frozen contract and authorize **Option B** spike.

**Do not** start Option B, Option C, or production adapter code until Sol records a decision.

---

## Executive summary

Spike Phase 1 demonstrates that Polars 0.46 can produce `arrow_array::RecordBatch` (workspace **59**) via the Arrow C Data Interface, with chunked reads and column projection across CSV, JSONL, and Parquet.

| Area | Spike result |
| --- | --- |
| Technical feasibility (Option A) | **PASS** |
| Contract compliance (no `unsafe`) | **FAIL** — see deviation above |
| Production implementation | **BLOCKED** |

Option B was **not** executed (Phase 1 stopped after Option A technical pass per sequential flow; production blocked by contract conflict).

---

## Reproduce

```bash
cd docs/issues/spikes/_scratch/option-a
cargo run
```

Expected: exit code **0**, final line `All Option A spike checks passed.`

**Scratch layout (committed for review):**

```text
docs/issues/spikes/_scratch/option-a/
├── .gitignore          # excludes target/
├── Cargo.toml
├── Cargo.lock
├── src/main.rs
└── fixtures/
    ├── sample.csv
    ├── sample.jsonl
    ├── multibatch.jsonl
    ├── nested.jsonl
    ├── empty_arrays.jsonl
    └── sample.parquet    # 3 row groups × 10 rows
```

---

## Spike environment

| Component | Version |
| --- | --- |
| Polars | 0.46.0 |
| polars-arrow | 0.46.0 |
| arrow-array / arrow-schema | 59.1.0 (`ffi` feature) |

---

## Option A bridge path

```text
Polars DataFrame
  → DataFrame::rechunk_to_record_batch(CompatLevel::newest())
  → per-column polars_arrow::ffi::export_array_to_c(Box<dyn Array>)
  → mem::transmute → arrow_array::ffi::FFI_ArrowArray
  → unsafe { from_ffi_and_data_type(...) }
  → arrow_array::RecordBatch
```

Schema dtype import:

```text
polars_arrow::Field
  → export_field_to_c
  → mem::transmute → FFI_ArrowSchema
  → DataType::try_from
  → Field::new + manual metadata copy from polars Field::metadata
```

### `unsafe` / `transmute` locations

All sites are in `src/main.rs` (production would isolate in `bridge/ffi.rs`):

| Site | Function | Line (approx.) | Construct |
| --- | --- | --- | --- |
| ABI array cast | `polars_array_to_arrow_rs` | ~88 | `unsafe { mem::transmute(polars_c_array) }` |
| FFI array import | `polars_array_to_arrow_rs` | ~92–94 | `unsafe { from_ffi_and_data_type(...) }` |
| ABI schema cast | `polars_field_to_arrow_rs_field` | ~113–114 | `unsafe { mem::transmute(polars_c_schema) }` |
| ABI schema cast (metadata proof) | `test_field_metadata_manual_copy` | ~318–319 | `unsafe { mem::transmute(...) }` |

### FFI ownership (single release)

| Step | Ownership |
| --- | --- |
| `export_array_to_c(polars_array)` | Consumes `Box<dyn PolarsArray>`; builds polars `ArrowArray` |
| `transmute` → `FFI_ArrowArray` | Moves ABI struct by value (no clone) |
| `from_ffi_and_data_type(arrow_c_array, dtype)` | **Consumes** `FFI_ArrowArray` once; builds `ArrayData` |
| `make_array` / `RecordBatch` drop | Releases arrow-rs buffers |

**Spike proof:** `test_ffi_ownership_and_repeated_drop` runs **200** create/drop cycles without crash or double-free.

**Forbidden:** calling `release` on the FFI struct after `from_ffi_and_data_type` returns.

---

## Test coverage matrix

| Test | What it proves |
| --- | --- |
| `test_csv_chunked_projection` | CSV `batched_borrowed` + byte `chunk_size`; projection `id,name`; 10 batches / 50 rows |
| `test_jsonl_multibatch_projection` | JSONL line-window reads (5 lines/batch); projection `id,value`; 6 batches / 30 rows |
| `test_parquet_multibatch_projection` | Parquet `scan_parquet` + `slice` windows; projection `id,score`; 3 batches / 30 rows (3 row groups) |
| `test_nullability` | CSV null cells (8 null names); JSONL null field (1 null name) |
| `test_empty_arrays` | JSONL row with `tags:[]` and row with `tags:["a"]` → `LargeList` through FFI |
| `test_nested_types` | JSONL `Struct` (`user`) + `LargeList` (`counts`) |
| `test_field_metadata_manual_copy` | Schema FFI alone drops metadata; manual copy restores `source=spike` |
| `test_ffi_ownership_and_repeated_drop` | 200 FFI import cycles, no crash |
| `test_obatch_memory_bound` | `max_batch_mem < file_bytes` on CSV chunked path (see caveat below) |

### O(batch) evidence

From `test_obatch_memory_bound` on `sample.csv` (577 bytes):

```text
file_bytes=577, max_batch_mem=530, batches=10
```

Each batch memory is computed as `sum(column.get_array_memory_size())`. Only one batch is held at a time in the spike loop.

**Caveat:** on tiny fixtures, a single batch can approach full file size because `chunk_size` is byte-based and the file is small. The evidence shows **no multi-batch accumulation** in memory, not asymptotic proof on large files. Production must enforce `ReadRequest::batch_size` and byte limits per contract.

---

## Format notes

### CSV

- `CsvReadOptions::with_chunk_size(n)` — **bytes**, not rows.
- Multi-batch via `CsvReader::batched_borrowed()` + `next_batches(1)`.

### JSONL

- `JsonReader` requires `JsonFormat::JsonLines` for NDJSON.
- Multi-batch spike uses **line-window reads** (`BufRead` + per-window `JsonReader`) to avoid full-file parse; production adapter should use Polars scan/batch APIs where available.

### Parquet

- Fixture `sample.parquet`: 30 rows, **3 row groups** (10 rows each).
- Multi-batch via `LazyFrame::scan_parquet` + repeated `slice(offset, 10)` + projection.

### Projection

All three formats tested with column subset before FFI bridge.

---

## Known limitations discovered during spike

| Issue | Severity | Notes |
| --- | --- | --- |
| `unsafe` + `transmute` required | **Contract BLOCKER** | See deviation section |
| Schema FFI drops field metadata | Medium | Must copy `polars_arrow::Field::metadata` manually |
| Polars `Null` dtype column FFI | High | All-null or empty-homogeneous columns inferred as `Null` fail `from_ffi_and_data_type` with `CDataInterface` error; adapter must reject or coerce before bridge |
| `Utf8View` in bridged output | Medium | Empty-list fixture produced `LargeList(Utf8View)`; contract MVP maps Utf8 — view types need explicit handling |
| No `DataFrame::to_arrow()` in Polars 0.46 | Info | Use `rechunk_to_record_batch` |

---

## Option B / C

| Option | Status |
| --- | --- |
| B — maintained compatibility bridge | **Not run** — awaiting Sol decision on Option A contract deviation |
| C — arrow-rs native readers | **Not authorized** |

---

## Dependencies (if Sol approves scoped `unsafe` exception)

Would be added to `stillflow-connector-local-tabular` only after approval:

```toml
polars = { version = "0.46", default-features = false, features = [
  "csv", "json", "parquet", "lazy", "dtype-struct", "dtype-array",
] }
polars-arrow = "0.46"
arrow-array = { workspace = true, features = ["ffi"] }
arrow-schema = { workspace = true, features = ["ffi"] }
```

---

## Revision history

| Date | Author | Change |
| --- | --- | --- |
| 2026-07-28 | Composer | Initial spike report (premature production approval — superseded) |
| 2026-07-28 | Composer | Expanded coverage + contract deviation; production blocked |
