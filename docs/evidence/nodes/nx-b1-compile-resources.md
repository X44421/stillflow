# NX-B1 compile-resource baseline measurements (#339)

Captured at the pre-change base `main@f401c2a` (merge of PR #351) with the
measurement harness `backend/crates/stillflow-plan/tests/nx_b1_resources.rs`
(`cargo test -p stillflow-plan --test nx_b1_resources -- --ignored --nocapture`).
The harness counts real allocations through a `#[global_allocator]` wrapper
and measures one compile after a warm-up compile.

- Toolchain: rustc 1.85.0 (4d91de4e4 2025-02-17), debug profile
- Environment: Linux x86_64 (WSL2), 2026-09-12
- Shapes: NX-C0 §9.3 — typical chain, 64-node wide schema, deep expression

## Results

| Shape | nodes × fields | wall (µs) | total alloc (B) | peak alloc (B) | nodeSchemas JSON (B) | output schema JSON (B) | canonical plan (B) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| typical-chain-6n-3f | 6 × 3 | 830 | 44 400 | 10 206 | 2 312 | 344 | 1 549 |
| wide-chain-64n-64f | 64 × 64 | 6 573 | 1 330 011 | 544 038 | 447 873 | 6 958 | 16 165 |
| wide-chain-64n-4096f | 64 × 4096 | 200 376 | 64 630 083 | 31 129 924 | 28 899 969 | 451 522 | 173 413 |
| deep-expression (~31) | 4 × 2 | 1 289 | 106 897 | 42 894 | 1 435 | 345 | 2 865 |

## Findings

1. **Schema-snapshot amplification is the dominant cost.** The 64-node
   4096-field shape stores a full `LogicalSchema` per product node: ~28.9 MB
   of serialized per-node schemas, ~31 MB peak allocations, ~200 ms wall —
   vs ~173 KB of canonical plan bytes. The serialized response would exceed
   the frozen 2 MiB response bound *after* all of the work and allocation
   had already happened.
2. **Per-field costs are stable across shapes**: ~110 serialized JSON bytes
   and ~119 in-memory bytes per schema-field instance (28.9 MB / 262 144
   instances; 31.1 MB / 262 144). The frozen accounting constant below is
   chosen above both.
3. **The config path cannot reach the 64-level expression bound.** Each
   `IsNull` costs two JSON nesting levels against the 64-level decode bound,
   so the deepest constructible expression through a node config is ~31 — a
   measured boundary finding; the deep shape uses depth 30 (2 is not
   reachable for a second reason: the derive node's expression must type-check).
4. **A 4096-column projection cannot be expressed in one source config**:
   it exceeds the 64 KiB config-byte bound (~155 KB of UUIDs), so the wide
   worst case uses an unprojected source plus trim nodes that keep every
   field in every intermediate schema.

## Decisions taken from the measurements (NX-C0 §9)

- **Accounting, not optimization.** No per-node schema representation change
  is adopted: the measured costs are correctness-preserving today, and the
  pathological shape is rejected up front instead (see the frozen constants
  in `check_compile_work`), keeping canonical bytes and execution behavior
  identical. An interior-pointer/shared-schema representation would be an
  L3 change to a public core type with no measured need once accounting
  rejects the amplification.
- **Frozen accounting constants** (charged in `check_compile_work`, rejected
  with the existing `NG_LIMIT_COMPILE_WORK` class before any snapshot is
  built):
  - `SCHEMA_SNAPSHOT_FIELD_COST_BYTES = 128` (measured: ~110 JSON / ~119
    in-memory; the constant covers both with headroom);
  - `MAX_SCHEMA_SNAPSHOT_ESTIMATE_BYTES = 2 MiB` (the frozen response bound
    of NX-C0 §9.1 — a compile whose estimated snapshot bytes exceed the
    response budget can never produce a deliverable response).
  The estimate is exact for the closed operator set: per-node output width
  is `columns.len()` for select, `input − 1` for drop-column, `input + 1`
  for derive-column, and `input` otherwise, with the source width being the
  projection length or the authorized schema width.
- **Response option** (§9.6): an optional, default-off `schemaDetail`
  request field (`full` | `compact`). `compact` deduplicates identical
  per-node schemas by digest so repeated wide schemas cost one copy plus
  references; the default response is byte-identical to today's.
