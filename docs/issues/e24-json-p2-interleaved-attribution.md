# Issue #131 Implementation Contract: E24-JSON-P2 interleaved attribution

> Status: Frozen overlay of Issue #131
> Risk: High (measurement harness only)
> Issue: [#131](https://github.com/X44421/stillflow/issues/131)
> Experimental base: PR #130 `1a6da5944e0e508c44c00fe1f408faf438b333df`
> Branch: `agent/e24-json-p2-interleaved-attribution`
> Last updated: 2026-08-26

This document mirrors Issue #131. The Issue remains authoritative if they diverge.

## 1. Objective

Decide whether P1's +19.08% 100-col ON/OFF delta was instrumentation cost or
OFF-then-ON thermal/order drift. Do not change P1 instrumentation or any JSON
algorithm.

## 2. Keep unchanged

- `src/read.rs` byte-identical to PR #130 (stride 64, batch-local counters,
  sampled per-row stages, exact decode/reorder)
- `parse_projected_object` black box
- Production `main@04966586192f8750a02790da988db71a28d82074`

## 3. Authorized change

Only `tests/read_json_phase_sampled_attribution.rs` scheduling/reporting.

After warmup of both modes, six measured runs per cell:

`OFF, ON, ON, OFF, OFF, ON`

Per-rep estimates, then medians. Same 10×100K and 100×100K cells.

## 4. Verdict (exactly one)

- |ON−OFF| median wall >3% either cell: `INTERLEAVED_ATTRIBUTION_INVALID_OVERHEAD`
- sampled rows <1000: `INTERLEAVED_ATTRIBUTION_INSUFFICIENT`
- 100-col coverage <80% or >115%: `INTERLEAVED_ATTRIBUTION_INCOMPLETE`
- largest 100-col stage ≥40%: `INTERLEAVED_ATTRIBUTION_DOMINANT_<STAGE>`
- else: `INTERLEAVED_ATTRIBUTION_MIXED`

No outcome authorizes optimization, merge, or a default-path flip.
