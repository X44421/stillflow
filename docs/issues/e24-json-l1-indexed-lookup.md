# Issue #124 Implementation Contract: E24-JSON-L1 indexed schema lookup

> Status: Frozen overlay of Issue #124
> Risk: High (connector JSON validation path)
> Issue: [#124](https://github.com/X44421/stillflow/issues/124)
> Authorized base: `main@04966586192f8750a02790da988db71a28d82074`
> Branch: `agent/e24-json-l1-indexed-lookup`
> Last updated: 2026-08-26

This document mirrors Issue #124. The Issue remains authoritative if they diverge.

## 1. Objective

Replace only per-key linear `schema.fields.iter().find` in top-level JSON
logical validation with a reader-level prebuilt name → field index. Framing,
Value materialization, selected-set construction, nested lookup, Polars JSON
tail, Arrow, batching, and public defaults stay unchanged.

## 2. Keep unchanged

- `JsonObjectStream` / `json_stream.rs`
- `ValidateFieldSeed`, `LogicalValueVisitor`, nested `fields.iter().find`
- per-row `BTreeSet` selected-set construction in `parse_projected_object`
- Polars re-encode tail, Engine, Storage, public APIs, default features

## 3. Authorized experiment

- Private feature `json-indexed-lookup` (off by default)
- Runtime `STILLFLOW_JSON_INDEXED_LOOKUP=1` selects indexed lookup
- `HashMap<String, usize>` built once in `prepare_reader`
- No new dependency

## 4. Focused A/B

WSL2 ext4, `--release`:

- 10 × 100K NDJSON
- 100 × 100K NDJSON
- warmup + 3 reps, linear vs indexed, same head

## 5. Kill gate

- 100-col <10%: `FOCUSED_LOOKUP_WEAK`
- 100-col ≥10% and 10-col regression >5%: `FOCUSED_LOOKUP_WIDE_ONLY`
- 100-col ≥10% and 10-col regression ≤5%: `FOCUSED_LOOKUP_PROMOTE_CANDIDATE`

No outcome authorizes merge or a default-path flip.
