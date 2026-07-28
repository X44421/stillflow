# Issue #6 — Local Tabular Connector

**Status:** Draft — Sol review required

**Risk:** high

**Implementation:** blocked until spike approval

**Branch:** `agent/mvp-006-local-tabular`

**Parent:** [#3](https://github.com/X44421/stillflow/issues/3) · **Issue:** [#6](https://github.com/X44421/stillflow/issues/6)

---

## Goal

Deliver `LocalTabularConnector`, an adapter for **local** CSV, JSONL, and Parquet files that implements the frozen `SourceConnector` contract:

- `test_connection`
- `discover`
- `inspect`
- `preview`
- `read_batches`

**Import** in this issue means: stream bounded `arrow_array::RecordBatch` values through `read_batches`. It does **not** include DatasetVersion registration, physical snapshot persistence, profiling, cleaning rules, or engine orchestration.

This is the first **MVP feature** task after Phase 0 (#4 + #5).

The decode/scan engine used by the adapter is **not predetermined**. A mandatory spike (below) selects among approved bridge strategies before any production code or dependency pins land.

---

## Non-goals

Explicitly out of scope for Issue #6:

| Excluded | Notes |
| --- | --- |
| TSV | Not in MVP scope |
| Plain JSON (non-line-delimited) | JSONL / NDJSON only |
| Excel / Calamine | Deferred (#7) |
| S3 / `object_store` / remote ranges | Deferred (#8) |
| SQL / SQLx | Deferred (#9) |
| DuckDB preview / materialization | Deferred (#10) |
| Profiling / issue detection | Later MVP work package |
| Cleaning rules / `RuleSpec` | Later MVP work package |
| `DatasetVersion` persistence | WP-02 |
| `DatasetSnapshot` physical write | WP-02 |
| Axum HTTP API | Not this issue |
| Frontend / UI changes | Frozen |
| `checkpoint` incremental reads | Return `Ok(None)` |
| Predicate pushdown (`SourceFilter`) | Reject via capability negotiation |
| `SamplingStrategy::Reservoir` / `Random` | Reject via capability negotiation |
| CSV lenient / skip-malformed-rows mode | Deferred until Rejected Dataset protocol exists |
| Schema override via connection config JSON | Use request fields (authorized below) or defer |

---

## Current architecture

```text
stillflow-api
      ↓
stillflow-engine
      ↓
stillflow-connectors   ← trait + registry only (frozen)
      ↓
stillflow-core         ← domain types, RequestContext, BatchStream
```

Frozen contracts from #5 (must not change without authorization in this contract):

- `SourceConnector` trait method signatures (unchanged)
- `ConnectorRegistry` as the only public dispatch entry
- `RequestContext` on all I/O request types
- `RawBatchStream::new`; registry attaches cancellation/deadline wrapping
- `PreviewRequest` limits: default 1,000 rows, max 10,000 rows, max 50 MiB bytes
- `ReadRequest::batch_size` range 1–65,536
- `ConnectorKind::LocalFile` (reuse; do **not** add a new enum variant)
- Arrow boundary: `arrow-array` + `arrow-schema` **59** only in `stillflow-core`

**Authorized by this contract (#6 public API supplement):**

Add optional schema override to read-path request types in `stillflow-core`:

```rust
// InspectRequest, PreviewRequest, ReadRequest
pub schema_override: Option<std::sync::Arc<arrow_schema::Schema>>
```

Schema override is generic read semantics, not LocalFile-specific configuration. It applies to the current operation only and does not mutate `SourceAsset` or `SourceConnection`.

Per `AGENTS.md` rule 4, format-specific decode logic lives in an **adapter crate**, not in `stillflow-connectors` trait/registry code.

---

## Frozen invariants

Implementation must preserve all of the following:

1. `stillflow-core` must not depend on Polars.
2. `stillflow-connectors` must not contain source-specific decode logic.
3. Apache Arrow `RecordBatch` (`arrow_array::RecordBatch`, workspace **59**) is the only structured output at the connector boundary.
4. Do not add the `arrow` meta crate.
5. Registry enforces `asset.connection_id == connection.id()` for asset-scoped operations.
6. Registry enforces capability negotiation before `inspect`, `preview`, and `read_batches`.
7. `discover` does **not** require `SchemaDiscovery` capability.
8. Secrets never appear in config, logs, events, or serialized payloads.
9. Unsupported optimizations return `UnsupportedCapability` — no silent fallback.
10. Stream termination: at most one terminal error, then `None`.
11. No fake streaming: `read_batches` must not full-materialize the dataset.
12. No `unsafe` in project code for Arrow bridging.
13. Do not downgrade workspace Arrow 59 or modify `SourceConnector` beyond the authorized `schema_override` fields.

---

## Supported format matrix

| Format | Extensions | Discover | Inspect schema | Preview | `read_batches` | Projection | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| CSV | `.csv` | Yes | Inferred (sampled) | Yes | Yes | Yes | UTF-8 + optional BOM |
| JSONL | `.jsonl`, `.ndjson` | Yes | Inferred (sampled) | Yes | Yes | Yes | One JSON object per line |
| Parquet | `.parquet` | Yes | Footer metadata | Yes | Yes | Yes | Column pruning at scan layer |

**Format recognition rule (frozen):**

1. Case-insensitive extension match is authoritative.
2. Extensionless files are **not** discovered.
3. `.json` (non-line-delimited) is **rejected** at inspect/read with `InvalidData`.
4. `.tsv` is **not** discovered (out of scope).
5. Parquet must pass magic-byte check (`PAR1` at head; footer validated on inspect/read).

---

## Connector configuration

`SourceConnection` uses `ConnectorKind::LocalFile`.

### Required config fields

```jsonc
{
  "allowedRoots": ["/absolute/path/to/data"]
}
```

| Field | Type | Required | Default | Semantics |
| --- | --- | --- | --- | --- |
| `allowedRoots` | `string[]` | **Yes** | — | Absolute directory roots. At least one. No `..` segments in config. |
| `followSymlinks` | `bool` | No | `false` | When `false`, any symlink component is rejected (see Path security). |
| `maxDiscoveryDepth` | `u32` | No | `8` | Max directory depth relative to each allowed root. |
| `maxDiscoveryEntries` | `u32` | No | `10_000` | Max files returned per `discover` call. |
| `defaultBatchSize` | `usize` | No | `8192` | Hint for callers; connector still validates `ReadRequest.batch_size`. |
| `schemaInference` | `object` | No | see below | Bounds inference sampling. |
| `formatDefaults` | `object` | No | `{}` | Connection-wide format defaults (delimiter, `hasHeader`, etc.). **No schema override here.** |

### `schemaInference` defaults

```jsonc
{
  "maxSampleRows": 1000,
  "maxSampleBytes": 1048576
}
```

### `formatDefaults` (connection-wide, optional)

```jsonc
{
  "csv": { "delimiter": ",", "hasHeader": true },
  "jsonl": {}
}
```

Schema override is **not** stored in connection config. Use `InspectRequest.schema_override`, `PreviewRequest.schema_override`, or `ReadRequest.schema_override`.

### Allowed roots validation

- Every root must be absolute.
- Reject roots containing `..` in config before resolution.
- Resolve each root with the **non-following** path algorithm (see Path security) and store the resolved absolute path.
- Roots should exist and be directories; `test_connection` may return `Degraded` if a root is missing.

---

## Discovery semantics

**Input:** `DiscoverRequest { context, parent_path: Option<String> }`

**Behaviour:**

1. `request.context.ensure_active()` before and during directory walk.
2. For each allowed root, walk recursively up to `maxDiscoveryDepth` using `symlink_metadata` per path component.
3. If `parent_path` is set, resolve it under each root (path security); skip roots where it does not exist.
4. Emit one `SourceAsset` per matching **file**.
5. Skip hidden files (name starts with `.`).
6. Skip/reject symlinks per Path security (`followSymlinks` default `false`).
7. Stop after `maxDiscoveryEntries`; emit finding `discover.truncated` when truncated.

**Asset fields:**

| Field | Value |
| --- | --- |
| `kind` | `AssetKind::File` |
| `name` | File basename |
| `locator.path` | Relative POSIX path from the matched root |
| `locator.container` | `None` |
| `connection_id` | owning connection UUID |

**Not required:** schema inference during discover.

---

## Inspection semantics

**Input:** `InspectRequest { context, asset, schema_override }`

**Registry precondition:** `Capability::SchemaDiscovery`.

**Behaviour:**

1. Resolve asset path under an allowed root (path security).
2. Detect format from extension + Parquet magic check.
3. Read file metadata: `size_bytes`, `modified_at`.
4. Determine schema:
   - If `schema_override` is `Some`, use it directly.
   - Else **Parquet:** footer / metadata only — no full row scan.
   - Else **CSV / JSONL:** bounded sample using `schemaInference` limits.
5. Populate `AssetMetadata { schema, format, size_bytes, row_count, modified_at, findings }`.

**`row_count` policy:**

| Format | `row_count` |
| --- | --- |
| Parquet | From footer metadata when available |
| CSV / JSONL | May be `None` with finding `inspect.row_count_unknown` when counting would exceed inference bounds |

**Original column names** are preserved in the Arrow schema.

---

## Preview semantics

**Input:** `PreviewRequest { context, asset, projection, filter, row_limit, byte_limit, sampling, schema_override }`

**Defaults (frozen, from core):**

- `row_limit`: default **1,000**, max **10,000**
- `byte_limit`: required, max **50 MiB** (see byte accounting below)
- `sampling`: only `SamplingStrategy::Head` is supported

**Behaviour:**

1. `request.context.ensure_active()` before work and between batch fetches.
2. Apply `schema_override` when present before scan/inference.
3. Apply projection at the earliest supported scan layer when `projection` is set.
4. Reject non-empty `filter` with `UnsupportedCapability`.
5. Reject `Reservoir` / `Random` with `UnsupportedCapability`.
6. Read at most `row_limit` rows.
7. Stop before adding a row or batch that would exceed `byte_limit` (Arrow memory accounting).
8. Set `rows_truncated` / `bytes_truncated` when limits hit.

### `byte_limit` accounting (frozen)

```text
byte_limit = cumulative estimated in-memory size of returned Arrow batches
```

**Use:** sum of per-batch buffer memory (e.g. `RecordBatch::get_array_memory_size()`).

**Do not use:**

- CSV / JSONL source file bytes on disk
- Parquet compressed file bytes
- JSON serialized string sizes
- Unrelated filesystem metadata

**Truncation rule:** if the next row or batch would exceed `byte_limit`, stop before including it and set `bytes_truncated = true`.

---

## Batch streaming semantics

**Input:** `ReadRequest { context, asset, projection, filter, checkpoint, batch_size, schema_override }`

**Import definition:** yield `RecordBatch` stream only. No dataset registration.

**Behaviour:**

1. Reject non-empty `filter` → `UnsupportedCapability`.
2. Reject non-`None` `checkpoint` → `UnsupportedCapability`.
3. Honour `batch_size` (1–65,536, validated by core).
4. Apply `schema_override` when present.
5. Open format-specific bounded scan; apply projection at scan layer when supported.
6. Iterate chunks of at most `batch_size` rows without full-file materialization.
7. Convert each chunk to `arrow_array::RecordBatch` via the **spike-approved** bridge.
8. Return `RawBatchStream`; registry wraps with `attach_request_context`.

**Stream semantics (frozen):**

| Rule | Requirement |
| --- | --- |
| Backpressure | Buffer at most **one** batch ahead of consumer |
| Cancellation | `ensure_active()` between chunks + registry stream wrapper |
| Terminal error | At most **one** `Err`, then `None` |
| Extra memory per step | O(`batch_size`), not O(file size) |

**Forbidden:** full-file `.collect()` / full-file materialization in `read_batches`.

---

## Arrow/Polars adapter

### Spike gate (BLOCKER — must complete before implementation)

**Status:** Not proven. Do **not** assume any bridge works until `cargo check` + fixture tests pass.

Polars 0.46 uses its own `polars-arrow 0.46` implementation ([polars 0.46 crate metadata](https://docs.rs/crate/polars/0.46.0)). That is a separate Arrow implementation from workspace `arrow-array` / `arrow-schema` **59**. Compatibility is **not** assumed from documentation, feature flags, or historical posts.

`df-interchange 0.3.3` lists Arrow **54–58** only ([df-interchange docs](https://docs.rs/df-interchange/latest/df_interchange/)) and is **not** a predetermined solution for Arrow 59.

Composer must publish spike results at `docs/issues/spikes/issue-006-arrow-bridge-spike.md` (or PR comment linked from the implementation PR) evaluating **all three** strategies:

#### Option A — Arrow C Data Interface

```text
Polars batch (or approved export)
  → Arrow C Data Interface export
  → arrow-array 59 RecordBatch import
```

#### Option B — Maintained compatibility bridge

```text
Polars batch
  → named bridge crate / API
  → arrow-array 59 RecordBatch
```

Only acceptable if spike proves **Polars 0.46 + Arrow 59** with real `cargo check` and fixture round-trip. The `polars-arrow` `arrow_rs` feature is a **candidate**, not an approved fact, until demonstrated.

#### Option C — arrow-rs native readers in connector

```text
arrow-rs CSV / JSON / Parquet readers (arrow 59 ecosystem)
  → arrow_array::RecordBatch directly
```

Polars remains reserved for later Cleaning Engine work. If A and B fail the gate, **stop** and escalate to Sol for Option C approval. Composer must not silently choose C without Sol sign-off.

### Spike acceptance criteria (all options)

| Criterion | Required |
| --- | --- |
| No full-file materialization | Yes |
| No `unsafe` in project code | Yes |
| Preserves nullability | Yes |
| Preserves nested types supported by MVP JSONL policy | Yes |
| Preserves field metadata where source provides it | Yes |
| Per-step extra memory | O(`batch_size`) |
| Fixture round-trip | CSV, JSONL, Parquet tiny fixtures |
| Workspace Arrow version | **59** (no downgrade) |
| Frozen traits | `SourceConnector` unchanged except authorized `schema_override` fields |

### If spike fails all options

Stop implementation. Report BLOCKER to Sol. Do not:

- downgrade Arrow 59
- modify `SourceConnector` beyond authorized request fields
- fake streaming with full `collect()`
- add `unsafe` bridging code

### Post-spike implementation rules

| Rule | Detail |
| --- | --- |
| Adapter location | `stillflow-connector-local-tabular` only |
| Core purity | No Polars / arrow-rs reader deps in `stillflow-core` |
| Meta crate | Do not add `arrow` meta crate |
| Unsupported dtypes | `InvalidData`, never silent string coercion |
| Dependency pins | Polars / bridge deps added only after spike selects a strategy |

### Dtype mapping (MVP, after successful bridge)

| Logical type | Arrow 59 |
| --- | --- |
| Boolean | `Boolean` |
| Int32 / Int64 | `Int32` / `Int64` |
| UInt32 / UInt64 | `UInt32` / `UInt64` |
| Float32 / Float64 | `Float32` / `Float64` |
| String / Utf8 | `Utf8` |
| Binary | `Binary` |
| Date / Datetime | `Date32` / `Timestamp` |
| Stable Struct | `Struct` |
| Stable List | `List` |
| Null-only column | `Null` |
| Object / Unknown / unstable union-like | **Error** `InvalidData` |

---

## Path security

All asset paths are resolved relative to an allowed root **without following symlinks by default**.

### Default resolution algorithm (frozen)

```text
1. Reject absolute asset locator paths.
2. Reject locator paths containing ".." components.
3. Start from resolved allowed root (root itself resolved without following symlinks).
4. For each relative path component:
   a. Join component to current path.
   b. Call symlink_metadata on the joined path.
   c. If metadata is a symlink:
        - if followSymlinks == false → reject immediately
        - if followSymlinks == true → resolve link target, verify target is under canonical allowed root, continue from target
   d. If metadata is a normal directory/file entry → continue
5. After full walk, verify final path is under allowed root prefix.
6. Open final path.
```

**Do not** use `std::fs::canonicalize` as the primary security primitive on the default path; it follows symlinks and conflicts with "do not follow symlinks" semantics.

### `followSymlinks: true` (future opt-in within this contract)

When enabled, resolved symlink target must still lie within the canonical allowed root prefix. Otherwise reject.

### Rejection errors

| Condition | `ErrorCategory` |
| --- | --- |
| Path escapes allowed root | `InvalidConfiguration` |
| `..` in input | `InvalidConfiguration` |
| Symlink when `followSymlinks == false` | `InvalidConfiguration` |
| Symlink target escapes root | `InvalidConfiguration` |
| Missing file | `NotFound` |
| Permission denied | `Authorization` or `InvalidConfiguration` |

### Windows notes

- Add tests or explicit platform notes for junctions / reparse points.
- If full parity is not implemented in #6, document as **platform limitation** in spike/PR and reject unknown reparse points conservatively.

---

## Error and warning semantics

### Fatal errors

| Code / case | Category | When |
| --- | --- | --- |
| Path escape / symlink policy violation | `InvalidConfiguration` | Security check fails |
| Unsupported extension | `InvalidData` | `.json`, `.tsv`, unknown |
| Invalid Parquet footer | `InvalidData` | Magic/metadata corrupt |
| UTF-8 decode failure | `InvalidData` | Invalid UTF-8 in CSV/JSONL |
| CSV malformed row | `InvalidData` | Strict mode only (see below) |
| JSONL malformed line / non-object line | `InvalidData` | See JSONL policy |
| JSONL unstable nested typing | `InvalidData` | See JSONL policy |
| Polars/bridge unsupported dtype | `InvalidData` | Cannot map to Arrow 59 |
| IO errors | `TransientSource` or `Internal` | Mapped per retryability |

### CSV malformed-row policy (frozen — strict only)

```text
Default and only behaviour: strict
```

- First malformed row → `InvalidData`
- Error must include safe location metadata: row number and/or byte offset
- Error must **not** include full raw row content (PII/secrets risk)
- No `ignoreMalformedLines`, no silent skip, no "successful import with dropped rows"
- Lenient / rejected-row handling deferred until Rejected Dataset protocol exists

### Inspection findings (non-fatal)

| Code | Severity | Meaning |
| --- | --- | --- |
| `csv.bom_stripped` | Info | UTF-8 BOM removed |
| `csv.delimiter_inferred` | Info | Delimiter auto-detected |
| `csv.infer_schema_truncated` | Warning | Inference hit sample bounds |
| `jsonl.schema_widened` | Warning | New fields appeared during sample |
| `jsonl.nested_field_as_struct` | Info | Stable nested object mapped to struct |
| `jsonl.nested_field_as_list` | Info | Stable list mapped to list |
| `parquet.row_count_from_metadata` | Info | Row count from footer |
| `inspect.row_count_unknown` | Warning | Row count not computed |
| `discover.truncated` | Warning | Hit `maxDiscoveryEntries` |

### JSONL nested-type policy (frozen)

**Allow:**

| Shape | Arrow mapping |
| --- | --- |
| Scalar fields | Primitive Arrow types |
| `List<stable single element type>` | `List` |
| `Struct<stable field set>` | `Struct` |

**Reject (`InvalidData`):**

- Same field mixing scalar / list / struct across lines or within sample
- Heterogeneous arrays whose element type cannot be stabilized within inference sample
- Union-like values without a stable schema
- Coercing unstable values to `Utf8` to force success

Non-object top-level JSON lines are **rejected** (not skipped).

---

## Cancellation and deadline semantics

| Operation | `RequestContext` source | Cooperative checks |
| --- | --- | --- |
| `test_connection` | `TestConnectionRequest.context` | Before each root stat |
| `discover` | `DiscoverRequest.context` | Every 100 entries during walk |
| `inspect` | `InspectRequest.context` | Before IO, after metadata, after sample |
| `preview` | `PreviewRequest.context` | Before scan, between batch fetches |
| `read_batches` | `ReadRequest.context` | Between chunks in adapter; registry wraps stream |
| `checkpoint` | `CheckpointRequest.context` | `ensure_active()` then `None` |

Blocking decode work runs on `tokio::task::spawn_blocking`. Do not start a new blocking job after terminal cancellation.

---

## Capability declaration

`LocalTabularConnector::capabilities()`:

| Capability | Value | Notes |
| --- | --- | --- |
| `schema_discovery` | `true` | inspect |
| `preview` | `true` | bounded head preview |
| `streaming` | `true` | `read_batches` |
| `incremental_read` | `false` | checkpoint always `None` |
| `predicate_pushdown` | `false` | reject `filter` |
| `column_projection` | `true` | when spike-selected engine supports it |
| `range_read` | `false` | local files only |
| `change_tracking` | `false` | — |

Registry negotiation (unchanged from #5):

- `inspect` requires `schema_discovery`
- `preview` requires `preview`; optional `column_projection`
- `read_batches` requires `streaming`; optional `column_projection`
- `discover` has **no** capability gate

---

## Format-specific decisions

### CSV

| Topic | Decision |
| --- | --- |
| Encoding | UTF-8 only; optional BOM strip (`csv.bom_stripped`) |
| Delimiter | Comma default; override via `formatDefaults.csv.delimiter` |
| Header | `hasHeader: true` default |
| Empty lines | Skip |
| Malformed rows | **Strict fatal** (see Error semantics) |
| Inference sample | `maxSampleRows` / `maxSampleBytes` |

### JSONL

| Topic | Decision |
| --- | --- |
| Line format | Exactly one JSON value per line |
| Top-level type | Must be JSON **object** |
| Nested typing | See JSONL nested-type policy |
| Inference sample | Same bounds as CSV |

### Parquet

| Topic | Decision |
| --- | --- |
| Schema | Footer metadata only for inspect |
| Preview | Row-group–aware bounded read |
| Projection | Column pruning at scan layer when supported by spike-selected engine |
| `read_batches` | Chunked / row-group–bounded iteration |
| Row count | From metadata when available |

---

## Files to modify

### Phase 0 — spike only (before production adapter)

```text
docs/issues/spikes/issue-006-arrow-bridge-spike.md
```

Optional temporary spike crate or `examples/` target — not merged as production code unless Sol approves.

### Phase 1 — after spike approval

**New crate:**

```text
backend/crates/stillflow-connector-local-tabular/
  Cargo.toml
  src/lib.rs
  src/connector.rs
  src/config.rs
  src/path_security.rs
  src/formats/{mod,csv,jsonl,parquet}.rs
  src/bridge.rs            # spike-selected strategy
  src/stream.rs
```

**Authorized core changes:**

```text
backend/crates/stillflow-core/src/domain/mod.rs      # schema_override on requests
backend/crates/stillflow-core/src/domain/preview.rs
backend/crates/stillflow-core/src/domain/read.rs
```

**Workspace wiring:**

```text
backend/Cargo.toml
backend/Cargo.lock
```

**Test fixtures:**

```text
backend/crates/stillflow-connector-local-tabular/tests/fixtures/
  csv/{orders,orders_bom,orders_semicolon,orders_malformed}.csv
  jsonl/{events,events_heterogeneous,events_bad_line,events_unstable_nested}.jsonl
  parquet/orders.parquet
```

### Must NOT modify (unless new contract)

- `SourceConnector` trait signatures (except via authorized request field additions)
- Frontend layout / CSS
- `stillflow-api` HTTP routes
- DatasetVersion / snapshot persistence

---

## Dependency changes

**Blocked until spike selects a strategy.** Do not add Polars or bridge crates to workspace `Cargo.toml` before spike approval.

Candidate dependencies (spike will confirm which are needed):

```toml
# Option A/B candidates — only if proven
polars = { version = "0.46", ... }
polars-arrow = { version = "0.46", features = ["arrow_rs"] }  # candidate only

# Option C candidates — only if Sol approves after A/B fail
# arrow-csv / arrow-json / parquet crate aligned with arrow 59
```

Workspace `arrow-array` / `arrow-schema` remain **59**.

**Forbidden:** `arrow` meta crate, `duckdb`, `sqlx`, `object_store`, `calamine`, `axum` in the adapter crate.

---

## Test matrix

| # | Area | Test | Type |
| --- | --- | --- | --- |
| S1 | Spike gate | A/B/C bridge compiles and round-trips fixtures | spike |
| S2 | Spike gate | proves O(batch) memory, no full collect | spike |
| T1 | Path security | `../` traversal rejected | unit |
| T2 | Path security | symlink component rejected (`followSymlinks: false`) | unit |
| T3 | Path security | valid relative path resolves under root | unit |
| T4 | Path security | Windows junction/reparse note or test | unit/platform |
| T5 | Discover | finds `.csv`, `.jsonl`, `.parquet`; ignores `.tsv`, `.json` | integration |
| T6 | Discover | respects `maxDiscoveryEntries` + warning | integration |
| T7 | Discover | honours cancellation mid-walk | async |
| T8 | Inspect | CSV schema inference within sample bounds | integration |
| T9 | Inspect | Parquet schema from footer without full read | integration |
| T10 | Inspect | `schema_override` on `InspectRequest` replaces inference | integration |
| T11 | Preview | default 1,000 rows, `rows_truncated` | integration |
| T12 | Preview | `byte_limit` uses Arrow memory accounting | integration |
| T13 | Preview | projection returns subset of columns | integration |
| T14 | Preview | rejects `filter` with `UnsupportedCapability` | integration |
| T15 | Read | batch stream chunk sizes ≤ `batch_size` | integration |
| T16 | Read | Parquet projection changes output schema | integration |
| T17 | Read | JSONL stable struct/list round-trip | integration |
| T18 | Read | JSONL unstable nested typing fails | integration |
| T19 | Read | malformed CSV fails with row/offset, no raw payload | integration |
| T20 | Read | cancellation during stream terminates once | async |
| T21 | Read | `schema_override` on `ReadRequest` honoured | integration |
| T22 | Registry | asset/connection mismatch rejected | integration |
| T23 | Registry | `discover` works without `schema_discovery` | integration |

---

## Acceptance commands

**Contract-only PR (this document):**

```bash
# docs change only — no backend test gate required
```

**Implementation PR (after spike approval):**

```bash
cd backend
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

---

## Forbidden changes

1. Begin implementation or add decode dependencies before spike approval.
2. Assume `polars-arrow arrow_rs → arrow 59` without proof.
3. Store schema override in connection config JSON.
4. Implement CSV lenient / skip-malformed-rows mode.
5. Full-file `.collect()` in `read_batches`.
6. Add `unsafe` bridging code.
7. Downgrade workspace Arrow 59.
8. Modify `SourceConnector` beyond authorized `schema_override` request fields.
9. Silent fallback when capabilities are unsupported.
10. Coerce unstable JSON values to `Utf8` to force reads.
11. Frontend layout/CSS changes.

---

## Open risks

| Risk | Mitigation |
| --- | --- |
| No proven Polars 0.46 → Arrow 59 bridge | Three-option spike; escalate Option C to Sol |
| `df-interchange` does not list Arrow 59 | Do not rely on it without verification |
| JSONL heterogeneous files fail strict policy | Explicit tests + typed `InvalidData` |
| Windows junction / reparse semantics | Dedicated test or documented limitation |
| `spawn_blocking` cancel latency | Between-chunk checks |
| Issue #6 GitHub body mentions TSV/JSON | This contract supersedes issue body for scope |

---

## Implementation sequence

**Blocked until Sol freezes this contract and spike is approved.**

1. **Sol review** — freeze contract.
2. **Spike gate** — evaluate Options A/B/C; publish `issue-006-arrow-bridge-spike.md`; obtain Sol approval for selected strategy (or Option C escalation).
3. **Core request extension** — add `schema_override` to inspect/preview/read requests.
4. **Adapter crate scaffold** + workspace wiring per spike outcome.
5. **Path security** — tests T1–T4.
6. **`test_connection` + `discover`** — tests T5–T7.
7. **`inspect`** — tests T8–T10.
8. **`preview`** — tests T11–T14.
9. **`read_batches` stream** — tests T15–T21.
10. **Registry integration** — tests T22–T23.
11. **Composer completion report** per `AGENTS.md`.

---

## Contract deviations

| Item | Authorization |
| --- | --- |
| `schema_override` on `InspectRequest`, `PreviewRequest`, `ReadRequest` | Authorized by this contract |
| All other frozen #5 contracts | Unchanged unless Sol issues a new contract |

---

## Review checklist for Sol

- [ ] Arrow bridge is hypothesis-only until spike proves otherwise
- [ ] Options A/B/C and escalation path are clear
- [ ] Schema override uses request fields, not connection config
- [ ] CSV strict-only policy is frozen
- [ ] Symlink algorithm does not rely on `canonicalize` follow semantics
- [ ] `byte_limit` defined as Arrow in-memory size
- [ ] JSONL nested allow/reject policy is frozen
- [ ] Implementation blocked until spike approval
- [ ] Scope matches MVP (#6 CSV/JSONL/Parquet local only)
