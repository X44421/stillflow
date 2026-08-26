# Issue #101 Implementation Contract: E24-B2JSON-A0 direct Arrow JSON prototype

> Status: Frozen
> Risk: High
> Issue: [#101](https://github.com/X44421/stillflow/issues/101)
> Authorized base: `main@04966586192f8750a02790da988db71a28d82074`
> Branch: `agent/issue-101-e24-b2json-direct-arrow`
> Last updated: 2026-08-26

This document mirrors the frozen Issue #101 contract. The Issue remains
authoritative if the two ever diverge.

## 1. Objective

Preserve StillFlow JSON/NDJSON framing and authoritative logical validation, and
replace only the downstream re-encode → Polars JSON decode → DataFrame → C-FFI
bridge with an incremental `arrow-json` 59 decoder that emits bounded Apache
Arrow `RecordBatch` values.

Valid terminal results after #117 G0: `FOCUSED_M3_PROMOTE` or
`FOCUSED_M3_WEAK`. The original `PROMOTE CANDIDATE` / `CLOSE EXPERIMENT`
labels applied to the superseded 8×30 matrix and must not be used for the
focused integration. Neither result authorizes production adoption,
Ready/merge, a default-path flip, `E23-OPT` transition, or work on #80/#91.

## 2. Keep unchanged

- `JsonObjectStream` framing and JSON-array / NDJSON boundaries (`json_stream.rs`
  is out of scope).
- `ProjectedObjectVisitor`, `ValidateFieldSeed`, and `LogicalValueVisitor` as
  the authoritative SchemaDrift / projection / nullability / range / nested /
  temporal / duplicate / unknown / missing-field validator.
- Public connector traits, `BatchEnvelope`, ordering, sequence, checkpoint,
  cancellation, `max_rows` no-lookahead, default features, CSV, TSV, Parquet,
  Engine, Storage, and frontend behavior.
- `io-metrics` remains private and off by default.

## 3. Authorized experiment

- One new third-party dependency: focused `arrow-json = "59"` (workspace-pinned,
  optional, private feature). Do not add the `arrow` meta crate.
- Private non-default Cargo feature `json-arrow-direct`. Default binaries keep
  the accepted legacy JSON path.
- Runtime switch `STILLFLOW_JSON_ARROW_DIRECT=1` selects the direct path only
  when the feature is compiled in. Unset/absent keeps legacy on the same head
  for A/B and semantic differential testing.
- Convert each already validated/projected JSON object with
  `arrow_json::reader::Decoder::serialize`, flushing bounded `RecordBatch`
  values at the existing fill bound (`INTERNAL_ROWS.min(batch_size)`). Feed rows
  incrementally; do not collect an unbounded `Vec<Map<...>>`.
- JSON may return Arrow batches internally without a Polars `DataFrame`.
  CSV/TSV/Parquet keep the current Polars path.

## 4. Explicit non-goals

- Removing or replacing the custom framer or logical validator.
- Optimizing JSON-array syntax validation, SIMD parsers, inference, cleaning,
  Engine/Storage, CSV/TSV/Parquet, or public APIs.
- Deleting the legacy JSON path, changing the default strategy, or merging the
  prototype.
- Using process-lifetime RSS as a correctness or promotion gate.

## 5. Semantic differential

Run legacy and direct strategies on the same fixtures and compare envelope
count, row count, sequence, Arrow schema, types, values, validity bitmaps,
nested offsets, projection order, empty projection row count, nullability,
unknown/duplicate fields, numeric/temporal/nested cases, JSON-array and NDJSON
syntax errors, error category, batch boundaries, and cancellation.

Any semantic mismatch blocks promotion. Do not weaken validation to paper over
it.

## 6. Mechanism evidence

With `json-arrow-direct` enabled and the runtime switch on:

- `json_framed_rows` and `json_handle_bytes` retain established meanings.
- `json_reencode_bytes == 0`.
- `json_polars_decode_invocations == 0`.
- Optional `json_arrow_flushes` proves bounded flush behavior.
- Peak buffered rows are bounded by the requested fill size.

## 7. Benchmark contract

Superseded by #117 G0 (2026-08-26). Do **not** run `8 cells × 2 strategies ×
30 reps` or any 1M-row / JSON-array cell.

Focused integration only, one exact head, legacy vs `json-arrow-direct`:

- `10 cols × 100K rows` NDJSON
- `100 cols × 100K rows` NDJSON
- warmup + 3 measured reps per strategy/cell
- persist `wall_samples_ms` (all 3 raw samples)
- report medians, ratios, `json_reencode_bytes` / `json_polars_decode_invocations`
  / `json_arrow_flushes`

`FOCUSED_M3_PROMOTE` if both cells keep a material connector-level median
gain (≥10% wall) with semantic parity already demonstrated.
`FOCUSED_M3_WEAK` if the module-level 67–81% M3 advantage collapses on this
hot path (either cell <10%, or a >5% regression). Neither label authorizes
merge.

## 8. Stop conditions

Stop and return to contract review for a public API / `BatchEnvelope` /
logical-schema / cancellation / error-category change; a second new dependency;
`unsafe`; unbounded buffering; another execution-engine responsibility; edits
outside the registry-authorized paths; changes to `json_stream.rs` or removal
of the current logical validator; or any Engine / Storage / #80 / #91 work.

## 9. Ownership note

`arrow-json` is used only as a bounded decoder after StillFlow validation. It
does not become a second cleaning-rule language. Polars remains the canonical
cleaning executor and the JSON legacy path on this head.
