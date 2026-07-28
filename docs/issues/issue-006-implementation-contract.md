# Issue #6 — Local Tabular Connector

**Status:** Draft — Sol review required

**Risk:** high

**Implementation:** blocked

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
| Schema override via connection config JSON | Use request fields (authorized below) |
| Following symlinks / junctions | Deferred to a future Issue; #6 always rejects |

---

## Engineering constraints (inlined)

`AGENTS.md` is **not present** on this branch / `main` at contract authoring time. The following rules are therefore **inlined** and binding for #6. When a development-control document lands later, it must not contradict these rules without a new contract.

### Dependency direction

```text
stillflow-api
      ↓
stillflow-engine
      ↓
stillflow-connector-local-tabular   ← adapter (new)
      ↓
stillflow-connectors                ← trait + registry only
      ↓
stillflow-core                      ← domain types + Arrow boundary types
```

Rules:

1. `stillflow-core`: domain types, errors, events, `RequestContext`, `BatchStream` only. No Polars / DuckDB / SQLx / Axum. No source-format decode.
2. Use `arrow-array` + `arrow-schema` **59**; never the `arrow` meta crate.
3. `stillflow-connectors`: trait + capabilities + registry + `RawBatchStream` only. Adapters go in separate crates.
4. Format-specific decode logic lives in `stillflow-connector-local-tabular`, not in `stillflow-connectors`.
5. The adapter **may** depend directly on workspace `arrow-array` / `arrow-schema` 59 for constructing `RecordBatch`. Public Arrow contracts remain defined by `stillflow-core`.
6. Unsupported optimizations return `ConnectorError` with `UnsupportedCapability` — never silent fallback.
7. Secrets via `CredentialRef` only; run `ensure_no_secret_fields` on connection config.
8. Do not modify `SourceConnector` method signatures unless this contract authorizes the change.
9. Prefer focused diffs; no drive-by refactors.
10. Do not modify frontend layout/CSS.
11. Report contract deviations, new deps, and any `unwrap` / `expect` in the completion summary.

### Tests required before implementation handoff

```bash
cd backend
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

### Stop and escalate

Stop if frozen connector contracts need changing beyond this document. File a contract revision request to Sol instead of redefining traits in an adapter PR.

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

- `SourceConnector` trait method signatures (unchanged except via authorized request field additions)
- `ConnectorRegistry` as the only public dispatch entry
- `RequestContext` on all I/O request types
- `RawBatchStream::new`; registry attaches cancellation/deadline wrapping
- `PreviewRequest` limits: default 1,000 rows, max 10,000 rows, max 50 MiB bytes
- `ReadRequest::batch_size` range 1–65,536
- `ConnectorKind::LocalFile` (reuse; do **not** add a new enum variant)
- Arrow boundary: `arrow-array` + `arrow-schema` **59** only as the public tabular interchange

**Authorized by this contract (#6 public API supplement):**

Add optional schema override to read-path request types in `stillflow-core`:

```rust
// InspectRequest, PreviewRequest, ReadRequest
pub schema_override: Option<std::sync::Arc<arrow_schema::Schema>>
```

Schema override is generic read semantics, not LocalFile-specific configuration. It applies to the current operation only and does not mutate `SourceAsset` or `SourceConnection`.

---

## Frozen invariants

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
14. Assets under multiple roots are uniquely identified by `(locator.container, locator.path)`.
15. `discover` never returns a silently truncated asset list.
16. Symlinks (and conservatively unknown reparse points) are always rejected in #6.

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
  "allowedRoots": [
    { "id": "uploads", "path": "/data/uploads" },
    { "id": "fixtures", "path": "/data/fixtures" }
  ]
}
```

| Field | Type | Required | Default | Semantics |
| --- | --- | --- | --- | --- |
| `allowedRoots` | `object[]` | **Yes** | — | At least one root. Each entry has unique `id` and absolute `path`. |
| `allowedRoots[].id` | `string` | **Yes** | — | Stable root identifier stored in `locator.container`. Non-empty; unique within connection. |
| `allowedRoots[].path` | `string` | **Yes** | — | Absolute directory path. No `..` segments in config. |
| `maxDiscoveryDepth` | `u32` | No | `8` | Max directory depth relative to each allowed root. |
| `maxDiscoveryEntries` | `u32` | No | `10_000` | Max files allowed per `discover` call. Exceeding → hard error (see Discovery). |
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

- Every root `path` must be absolute.
- Reject paths containing `..` in config before resolution.
- Every root `id` must be non-empty and unique within the connection.
- Resolve each root with the **non-following** path algorithm (see Path security) and store the resolved absolute path.
- Root accessibility is reported by `test_connection` (see below).

### `test_connection` results (frozen)

| Condition | Result |
| --- | --- |
| All configured roots exist, are directories, and are accessible | `ConnectionStatus::Ok` |
| At least one root accessible, at least one missing / inaccessible | `ConnectionStatus::Degraded { warnings }` listing inaccessible root ids |
| No root accessible | Fatal error: `NotFound` if missing, `Authorization` if permission denied (prefer the dominant cause; if mixed, `NotFound` with message covering inaccessible roots) |

---

## Discovery semantics

**Input:** `DiscoverRequest { context, parent_path: Option<String> }`

**Behaviour:**

1. `request.context.ensure_active()` before and during directory walk.
2. For each allowed root, walk recursively up to `maxDiscoveryDepth` using `symlink_metadata` per path component.
3. If `parent_path` is set, resolve it under each root (path security); skip roots where it does not exist.
4. Emit one `SourceAsset` per matching **file**.
5. Skip hidden files (name starts with `.`).
6. Reject / skip any symlink or unknown reparse point (always).
7. If the number of matching files would exceed `maxDiscoveryEntries`, **abort the entire discover** with `InvalidConfiguration`. Do **not** return a partial `Vec`.

**Truncation policy (frozen — no warning channel):**

`discover()` returns `ConnectorResult<Vec<SourceAsset>>` with no findings/warnings field. Therefore:

```text
matching files > maxDiscoveryEntries
  → Err(InvalidConfiguration)
  → message: suggest narrowing parent_path or raising maxDiscoveryEntries
  → no incomplete Vec is returned
```

Callers must never treat a successful `Ok(vec)` as a silently truncated listing.

**Asset fields:**

| Field | Value |
| --- | --- |
| `kind` | `AssetKind::File` |
| `name` | File basename |
| `locator.path` | Relative POSIX path **within** the matched root |
| `locator.container` | `Some(root.id)` — **required** for multi-root uniqueness |
| `connection_id` | owning connection UUID |

**Resolution rule for inspect / preview / read:**

```text
root = connection.allowedRoots.find(r => r.id == asset.locator.container)
path = resolve(root.path, asset.locator.path)  // path security
```

Missing / unknown `locator.container` → `InvalidConfiguration`.

**Not required:** schema inference during discover.

---

## Inspection semantics

**Input:** `InspectRequest { context, asset, schema_override }`

**Registry precondition:** `Capability::SchemaDiscovery`.

**Behaviour:**

1. Resolve asset via `locator.container` + `locator.path` (path security).
2. Detect format from extension + Parquet magic check.
3. Read file metadata: `size_bytes`, `modified_at`.
4. Determine schema:
   - If `schema_override` is `Some`, validate compatibility with inferred/footer schema when available; incompatible override → `SchemaDrift`.
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
9. Enforce post-sample schema drift rules during scan (see Error semantics).

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

If the **first** row/batch alone exceeds `byte_limit`, return empty batches with `bytes_truncated = true` and `rows_returned = 0` (or fail with `InvalidConfiguration` if the connector cannot produce a zero-row schema-bearing result). Prefer returning schema + empty batches + flags when schema is known.

---

## Batch streaming semantics

**Input:** `ReadRequest { context, asset, projection, filter, checkpoint, batch_size, schema_override }`

**Import definition:** yield `RecordBatch` stream only. No dataset registration.

**Behaviour:**

1. Reject non-empty `filter` → `UnsupportedCapability`.
2. Reject non-`None` `checkpoint` → `UnsupportedCapability` (`incremental_read` not supported). `checkpoint()` method itself returns `Ok(None)` after `ensure_active()`.
3. Honour `batch_size` (1–65,536, validated by core).
4. Apply `schema_override` when present.
5. Resolve asset via `locator.container` + `locator.path`.
6. Open format-specific bounded scan; apply projection at scan layer when supported.
7. Iterate chunks of at most `batch_size` rows without full-file materialization.
8. Convert each chunk to `arrow_array::RecordBatch` via the **spike-approved** bridge.
9. Enforce schema drift rules against the inspect / override schema.
10. Return `RawBatchStream`; registry wraps with `attach_request_context`.

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

### Spike phases (frozen)

```text
Spike Phase 1 — actually compile and verify Options A and B only
  ├─ A succeeds → stop; report A as selected
  ├─ B succeeds → stop; report B as selected
  └─ A and B both fail → STOP and report BLOCKER to Sol
       (do not begin Option C)

After Sol explicitly approves Option C:
Spike Phase 2 — actually compile and verify Option C
  ├─ C succeeds → report C as selected
  └─ C fails → STOP; escalate again
```

Composer must **not** pre-require Option C compilation success in Phase 1 tests.

Publish results at `docs/issues/spikes/issue-006-arrow-bridge-spike.md`.

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

Polars remains reserved for later Cleaning Engine work. Option C requires **Sol approval after Phase 1 failure**. Composer must not silently choose C.

### Spike acceptance criteria

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

### If spike fails

Stop implementation. Report BLOCKER to Sol. Do not:

- downgrade Arrow 59
- modify `SourceConnector` beyond authorized request fields
- fake streaming with full `collect()`
- add `unsafe` bridging code
- begin Option C without Sol approval

### Post-spike implementation rules

| Rule | Detail |
| --- | --- |
| Adapter location | `stillflow-connector-local-tabular` only |
| Core purity | No Polars / arrow-rs reader deps in `stillflow-core` |
| Connector purity | No format decode in `stillflow-connectors` |
| Meta crate | Do not add `arrow` meta crate |
| Unsupported dtypes | `InvalidData`, never silent string coercion |
| Dependency pins | Decode/bridge deps added only after spike selects a strategy |
| Arrow deps in adapter | Workspace `arrow-array` / `arrow-schema` 59 allowed |

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

All asset paths are resolved relative to the root identified by `locator.container`.

### Resolution algorithm (frozen — no symlink following)

```text
1. Require locator.container = Some(root_id); else InvalidConfiguration.
2. Lookup root by id in connection.allowedRoots; else InvalidConfiguration.
3. Reject absolute locator.path values.
4. Reject locator.path containing ".." components.
5. Start from resolved allowed root path (root itself resolved without following symlinks).
6. For each relative path component:
   a. Join component to current path.
   b. Call symlink_metadata on the joined path.
   c. If metadata is a symlink OR unknown reparse point / junction → reject immediately.
   d. If metadata is a normal directory/file entry → continue.
7. After full walk, verify final path is under the allowed root prefix.
8. Open final path.
```

**Do not** use `std::fs::canonicalize` as the primary security primitive; it follows symlinks.

### Symlink policy for #6 (frozen)

```text
Always reject symlinks.
No followSymlinks config flag in MVP.
```

Allowing symlink follow introduces TOCTOU, Windows junction, and reparse-point risks. Track a follow-up Issue if needed later.

### Rejection errors

| Condition | `ErrorCategory` |
| --- | --- |
| Unknown / missing `locator.container` | `InvalidConfiguration` |
| Path escapes allowed root | `InvalidConfiguration` |
| `..` in input | `InvalidConfiguration` |
| Symlink / junction / unknown reparse point | `InvalidConfiguration` |
| Missing file | `NotFound` |
| Permission denied | `Authorization` or `InvalidConfiguration` |

### Windows notes

- Treat junctions / reparse points as reject-by-default in #6.
- Document as **platform limitation** if finer classification is deferred; still must not follow them.

---

## Error and warning semantics

### Fatal errors

| Code / case | Category | When |
| --- | --- | --- |
| Path escape / symlink / missing container | `InvalidConfiguration` | Security / locator check fails |
| Discover exceeds `maxDiscoveryEntries` | `InvalidConfiguration` | Would truncate listing |
| Unsupported extension | `InvalidData` | `.json`, `.tsv`, unknown |
| Invalid Parquet footer | `InvalidData` | Magic/metadata corrupt |
| Non-UTF-8 text | `InvalidData` | Invalid UTF-8 in CSV/JSONL |
| CSV malformed row | `InvalidData` | Strict mode |
| JSONL malformed / non-object line | `InvalidData` | See JSONL policy |
| JSONL unstable nested typing | `InvalidData` | See JSONL policy |
| Post-sample / override schema conflict | `SchemaDrift` | See drift rules |
| Unsupported dtype at boundary | `InvalidData` | Cannot map to Arrow 59 |
| Unsupported sampling / filter / checkpoint request | `UnsupportedCapability` | Capability negotiation |
| IO errors | `TransientSource` or `Internal` | Mapped per retryability |

### Schema drift rules (frozen)

Relative to the schema established by inspect inference / footer / `schema_override`:

| Event during preview / read | Result |
| --- | --- |
| Missing field present in schema | Fill with Null (nullable fields); if field is non-nullable and no value → `SchemaDrift` |
| New field appears after sample / not in override | `SchemaDrift` — do **not** ignore or coerce to Utf8 |
| Existing field type conflicts with established schema | `SchemaDrift` |
| `schema_override` incompatible with footer / stable inferred schema | `SchemaDrift` |

Never silently drop new columns. Never force-cast conflicting types to Utf8 to continue.

### CSV malformed-row policy (frozen — strict only)

```text
Default and only behaviour: strict
```

- First malformed row → `InvalidData`
- Error must include safe location metadata: row number and/or byte offset
- Error must **not** include full raw row content (PII/secrets risk)
- No lenient skip mode in #6

### Inspection findings (non-fatal)

| Code | Severity | Meaning |
| --- | --- | --- |
| `csv.bom_stripped` | Info | UTF-8 BOM removed |
| `csv.delimiter_inferred` | Info | Delimiter auto-detected |
| `csv.infer_schema_truncated` | Warning | Inference hit sample bounds |
| `jsonl.schema_widened` | Warning | New fields appeared **within** sample (schema still stabilized) |
| `jsonl.nested_field_as_struct` | Info | Stable nested object mapped to struct |
| `jsonl.nested_field_as_list` | Info | Stable list mapped to list |
| `parquet.row_count_from_metadata` | Info | Row count from footer |
| `inspect.row_count_unknown` | Warning | Row count not computed |

Note: there is **no** `discover.truncated` finding — discover over-limit is a hard error.

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
| `test_connection` | `TestConnectionRequest.context` | Before each root access check |
| `discover` | `DiscoverRequest.context` | Every 100 entries during walk |
| `inspect` | `InspectRequest.context` | Before IO, after metadata, after sample |
| `preview` | `PreviewRequest.context` | Before scan, between batch fetches |
| `read_batches` | `ReadRequest.context` | Between chunks in adapter; registry wraps stream |
| `checkpoint` | `CheckpointRequest.context` | `ensure_active()` then `Ok(None)` |

Blocking decode work runs on `tokio::task::spawn_blocking`. Do not start a new blocking job after terminal cancellation.

---

## Capability declaration

`LocalTabularConnector::capabilities()`:

| Capability | Value | Notes |
| --- | --- | --- |
| `schema_discovery` | `true` | inspect |
| `preview` | `true` | bounded head preview |
| `streaming` | `true` | `read_batches` |
| `incremental_read` | `false` | `checkpoint()` always `None` |
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
| Encoding | UTF-8 only; optional BOM strip (`csv.bom_stripped`); non-UTF-8 → `InvalidData` |
| Delimiter | Comma default; override via `formatDefaults.csv.delimiter` |
| Header | `hasHeader: true` default |
| Empty file | Valid; schema may be empty / header-only; zero rows |
| Empty lines | Skip |
| Malformed rows | **Strict fatal** |
| Inference sample | `maxSampleRows` / `maxSampleBytes` |

### JSONL

| Topic | Decision |
| --- | --- |
| Line format | Exactly one JSON value per line |
| Top-level type | Must be JSON **object** |
| Empty file | Valid; zero rows; empty or override schema |
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
backend/crates/stillflow-core/src/domain/mod.rs      # schema_override on InspectRequest
backend/crates/stillflow-core/src/domain/preview.rs  # schema_override on PreviewRequest
backend/crates/stillflow-core/src/domain/read.rs     # schema_override on ReadRequest
```

**Workspace wiring:**

```text
backend/Cargo.toml
backend/Cargo.lock
```

**Test fixtures:**

```text
backend/crates/stillflow-connector-local-tabular/tests/fixtures/
  csv/{orders,orders_bom,orders_semicolon,orders_malformed,orders_empty,orders_non_utf8}.csv
  jsonl/{events,events_heterogeneous,events_bad_line,events_unstable_nested,events_empty}.jsonl
  parquet/orders.parquet
  multi_root/
    uploads/data/orders.csv
    fixtures/data/orders.csv
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
# Option A/B candidates — only if proven in Spike Phase 1
polars = { version = "0.46", ... }
polars-arrow = { version = "0.46", features = ["arrow_rs"] }  # candidate only

# Option C candidates — only after Sol approves Spike Phase 2
# arrow-csv / arrow-json / parquet crate aligned with arrow 59
```

Workspace `arrow-array` / `arrow-schema` remain **59**.

Adapter dependency direction:

```text
stillflow-connector-local-tabular
        ↓
stillflow-connectors
        ↓
stillflow-core
```

The adapter may also depend on workspace `arrow-array` / `arrow-schema` 59 directly.

**Forbidden:** `arrow` meta crate, `duckdb`, `sqlx`, `object_store`, `calamine`, `axum` in the adapter crate.

---

## Test matrix

| # | Area | Test | Type |
| --- | --- | --- | --- |
| S1 | Spike Phase 1 | Options A and B compile + round-trip fixtures (C not required) | spike |
| S2 | Spike Phase 1 | proves O(batch) memory, no full collect | spike |
| S3 | Spike Phase 2 | Option C only after Sol approval | spike |
| T1 | Path security | `../` traversal rejected | unit |
| T2 | Path security | symlink component rejected | unit |
| T3 | Path security | valid relative path resolves under selected root | unit |
| T4 | Path security | Windows junction/reparse rejected or documented | unit/platform |
| T5 | Multi-root | two roots with same relative path resolve via `locator.container` | integration |
| T6 | Discover | exceeds `maxDiscoveryEntries` → `InvalidConfiguration`, no partial Vec | integration |
| T7 | Discover | finds `.csv`, `.jsonl`, `.parquet`; ignores `.tsv`, `.json` | integration |
| T8 | Discover | honours cancellation mid-walk | async |
| T9 | Connection | all roots accessible → `Ok` | integration |
| T10 | Connection | some roots inaccessible → `Degraded` | integration |
| T11 | Connection | no roots accessible → `NotFound` / `Authorization` | integration |
| T12 | Inspect | CSV schema inference within sample bounds | integration |
| T13 | Inspect | Parquet schema from footer without full read | integration |
| T14 | Inspect | `schema_override` replaces inference when compatible | integration |
| T15 | Inspect | incompatible `schema_override` → `SchemaDrift` | integration |
| T16 | Preview | default 1,000 rows, `rows_truncated` | integration |
| T17 | Preview | `byte_limit` uses Arrow memory accounting | integration |
| T18 | Preview | first row already exceeds `byte_limit` → truncate flags / empty batches | integration |
| T19 | Preview | projection returns subset of columns | integration |
| T20 | Preview | rejects `filter` with `UnsupportedCapability` | integration |
| T21 | Preview | rejects `Reservoir` / `Random` sampling | integration |
| T22 | Read | batch stream chunk sizes ≤ `batch_size` | integration |
| T23 | Read | Parquet projection changes output schema | integration |
| T24 | Read | JSONL stable struct/list round-trip | integration |
| T25 | Read | JSONL unstable nested typing fails | integration |
| T26 | Read | malformed CSV fails with row/offset, no raw payload | integration |
| T27 | Read | non-UTF-8 CSV/JSONL → `InvalidData` | integration |
| T28 | Read | BOM CSV strips BOM and reads successfully | integration |
| T29 | Read | empty CSV / empty JSONL succeed with zero rows | integration |
| T30 | Read | post-sample new field → `SchemaDrift` | integration |
| T31 | Read | post-sample type conflict → `SchemaDrift` | integration |
| T32 | Read | cancellation during stream terminates once | async |
| T33 | Read | `schema_override` on `ReadRequest` honoured | integration |
| T34 | Checkpoint | `checkpoint()` returns `Ok(None)` | integration |
| T35 | Registry | asset/connection mismatch rejected | integration |
| T36 | Registry | `discover` works without `schema_discovery` | integration |

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

1. Begin Spike Phase 1 before Sol marks this contract `FROZEN`.
2. Begin implementation or add decode dependencies before spike approval.
3. Assume `polars-arrow arrow_rs → arrow 59` without proof.
4. Begin Option C without Sol approval after Phase 1 failure.
5. Store schema override in connection config JSON.
6. Implement CSV lenient / skip-malformed-rows mode.
7. Return truncated discover results with a soft warning.
8. Leave `locator.container` empty when multiple roots are configured.
9. Follow symlinks / junctions in #6.
10. Full-file `.collect()` in `read_batches`.
11. Add `unsafe` bridging code.
12. Downgrade workspace Arrow 59.
13. Modify `SourceConnector` beyond authorized `schema_override` request fields.
14. Silent fallback when capabilities are unsupported.
15. Coerce unstable JSON values or drifted fields to `Utf8` to force reads.
16. Frontend layout/CSS changes.

---

## Open risks

| Risk | Mitigation |
| --- | --- |
| No proven Polars 0.46 → Arrow 59 bridge | Phase 1 A/B spike; escalate C to Sol |
| `df-interchange` does not list Arrow 59 | Do not rely on it without verification |
| JSONL heterogeneous files fail strict policy | Explicit tests + typed `InvalidData` / `SchemaDrift` |
| Windows junction / reparse semantics | Reject by default; document limitation |
| `spawn_blocking` cancel latency | Between-chunk checks |
| Issue #6 GitHub body mentions TSV/JSON | This contract supersedes issue body for scope |
| `AGENTS.md` missing on branch | Constraints inlined in this document |

---

## Implementation sequence

**Blocked until Sol freezes this contract.**

1. **Sol review** — mark contract `FROZEN`.
2. **Spike Phase 1** — evaluate Options A and B only; publish spike notes; stop if both fail.
3. **Optional Spike Phase 2** — only after Sol approves Option C.
4. **Core request extension** — add `schema_override` to inspect/preview/read requests.
5. **Adapter crate scaffold** + workspace wiring per spike outcome.
6. **Config + path security + multi-root locator** — tests T1–T5, T9–T11.
7. **`discover`** — tests T6–T8.
8. **`inspect`** — tests T12–T15.
9. **`preview`** — tests T16–T21.
10. **`read_batches` stream** — tests T22–T34.
11. **Registry integration** — tests T35–T36.
12. **Completion summary** — list new deps, contract deviations, and any `unwrap` / `expect`.

---

## Contract deviations

| Item | Authorization |
| --- | --- |
| `schema_override` on `InspectRequest`, `PreviewRequest`, `ReadRequest` | Authorized by this contract |
| `allowedRoots` as `{ id, path }[]` with `locator.container = Some(root.id)` | Authorized by this contract |
| All other frozen #5 contracts | Unchanged unless Sol issues a new contract |

---

## Review checklist for Sol

- [ ] Multi-root assets uniquely identified via `locator.container`
- [ ] Discover over-limit is hard `InvalidConfiguration`, no partial Vec
- [ ] `AGENTS.md` absence handled by inlined engineering constraints
- [ ] Spike Phase 1 = A/B only; Phase 2 = C after Sol approval
- [ ] Schema drift rules frozen
- [ ] `test_connection` Ok / Degraded / fatal outcomes frozen
- [ ] Symlinks always rejected in #6
- [ ] Dependency direction includes adapter → connectors → core
- [ ] Test matrix covers multi-root, drift, empty files, BOM, non-UTF-8, byte_limit edge, checkpoint
- [ ] Implementation remains blocked until `FROZEN`
