# Issue #126 Implementation Contract: E24-JSON-P0 phase attribution

> Status: Frozen overlay of Issue #126
> Risk: High (connector JSON read path instrumentation)
> Issue: [#126](https://github.com/X44421/stillflow/issues/126)
> Authorized base: `main@04966586192f8750a02790da988db71a28d82074`
> Branch: `agent/e24-json-p0-phase-attribution`
> Last updated: 2026-08-26

This document mirrors Issue #126. The Issue remains authoritative if they diverge.

## 1. Objective

Attribute real NDJSON connector wall time across the existing production loop.
No JSON algorithm, data-structure, or default-path change.

## 2. Keep unchanged

- `parse_projected_object` internals (black box: selected-set, Visitor, lookup,
  duplicate/missing, type checks, `Value`, projection reorder inside that fn)
- `JsonObjectStream` framing
- Polars re-encode/decode algorithm and `reorder_frame`
- Engine, Storage, public APIs, default features

## 3. Authorized measurement

- Private feature `io-metrics` (already present; still off by default)
- Runtime `STILLFLOW_JSON_PHASE_TIMING=1` enables `Instant` around five stages
- Default: do not call `Instant::now` for these stages
- No new dependency
- Dump counters: `json_phase_frame_ns`, `json_phase_project_validate_ns`,
  `json_phase_reencode_ns`, `json_phase_polars_decode_ns`,
  `json_phase_reorder_ns`

Stages:

1. `frame` — `reader.next_raw_object(...)`
2. `project_validate` — `parse_projected_object(...)`
3. `reencode` — `serde_json::to_writer(...)` + newline
4. `polars_decode` — `JsonReader::finish()`
5. `reorder` — `reorder_frame(...)`

`other = wall - sum(stages)`.

## 4. Focused matrix

WSL2 ext4, rustc 1.85.0, `--release`, `--features io-metrics`:

- 10 × 100K NDJSON
- 100 × 100K NDJSON

Per cell: warmup; 3 reps timing off; 3 reps timing on.

## 5. Verdict (exactly one)

- timing-on vs timing-off median wall >5% either cell:
  `ATTRIBUTION_INVALID_OVERHEAD`
- else 100-col coverage `(sum stages)/wall` <80%:
  `ATTRIBUTION_INCOMPLETE`
- else largest 100-col stage ≥40% wall:
  `ATTRIBUTION_DOMINANT_<STAGE>`
- else: `ATTRIBUTION_MIXED`

No outcome authorizes an optimization, merge, or default-path flip.
