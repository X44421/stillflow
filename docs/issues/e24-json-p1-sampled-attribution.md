# Issue #129 Implementation Contract: E24-JSON-P1 sampled phase attribution

> Status: Frozen overlay of Issue #129
> Risk: High (connector JSON read path instrumentation)
> Issue: [#129](https://github.com/X44421/stillflow/issues/129)
> Authorized base: `main@04966586192f8750a02790da988db71a28d82074`
> Branch: `agent/e24-json-p1-sampled-attribution`
> Last updated: 2026-08-26

This document mirrors Issue #129. The Issue remains authoritative if they diverge.

## 1. Objective

Repeat real-path NDJSON phase attribution with sampled per-row timing so
instrumentation overhead can pass the validity gate. Measurement only: no JSON
algorithm change. Do not interpret P0 stage shares.

## 2. Keep unchanged

- `parse_projected_object` internals (black box)
- Framing, Visitor/DOM, lookup, selected-set, Arrow, Polars tail, JSON array
- Default path with sampling env unset

## 3. Authorized measurement

- Private feature `io-metrics`; runtime `STILLFLOW_JSON_PHASE_SAMPLE=1`
- Default: no `Instant::now` for these stages
- Per-row stages sampled every 64 logical rows only:
  `frame`, `project_validate`, `reencode`
- Sampled nanoseconds accumulate in batch-local integers; atomics/metrics
  publish at most once per produced batch
- `polars_decode` and `reorder` timed exactly once per produced batch
- Estimate per ingest: `sampled_ns * total_rows / sampled_rows`
- Medians are over per-rep estimates, not over pooled sampled nanoseconds

## 4. Focused matrix

WSL2 ext4, rustc 1.85.0, `--release`, `--features io-metrics`:

- 10 × 100K NDJSON
- 100 × 100K NDJSON

Per cell: warmup; 3 reps sampling off; 3 reps sampling on.

## 5. Verdict (exactly one)

- sampling-on vs off median wall |delta| >3% either cell:
  `SAMPLED_ATTRIBUTION_INVALID_OVERHEAD`
- else sampled rows <1000 in a 100K-row cell:
  `SAMPLED_ATTRIBUTION_INSUFFICIENT`
- else 100-col coverage <80% or >115%:
  `SAMPLED_ATTRIBUTION_INCOMPLETE`
- else largest 100-col stage ≥40% wall:
  `SAMPLED_ATTRIBUTION_DOMINANT_<STAGE>`
- else: `SAMPLED_ATTRIBUTION_MIXED`

No outcome authorizes optimization, merge, or a default-path flip.
