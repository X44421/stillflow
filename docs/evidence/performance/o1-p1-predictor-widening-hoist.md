# O1-P1 — Predictor variable-width scan: type-resolution hoist evidence

- Version: 1
- Date: 2026-09-05
- Issue: #297 (`[O1-P1] Predictor variable-width scan optimization`)
- Exact measured head: `e895a9a7a60c237c01d8439c2016261d27215f7f` (branch
  `agent/issue-297-o1-p1-predictor-widening`; the implementation commit). The
  only post-measurement commit on this branch is this evidence note itself.
- Baseline: `main@0af8f38a28dce5dccbe357f77bbb3e2048e36982`, measured in the
  same worktree with the implementation stashed, same target dir, same
  command, back-to-back.
- Scope: behavior-preserving refactor of the widening-scan width computation
  (`max_variable_width`, `variable_data_bytes` fallback loop). The concrete
  array-type decision (`value_width`'s per-row four-way `downcast_ref` chain)
  is loop-invariant and is now resolved once per array via a `WidthSource`
  enum; per-row work is the identical null-check + width read. Predicted byte
  counts, final slices, errors, and the instrumentation counters
  (`record_max_variable_width_scan(rows, value_bytes)`) are byte-identical by
  construction; the full engine suite verifies.

## 1. Machine and measurement discipline

Same machine as the O0 round (6 vCPU i3-12100F, WSL2, ext4, `--release`,
page cache warm). Both matrices ran the #292 harness as-is
(`cargo test --release -p stillflow-engine --lib --features predict-metrics
-- predict_metrics_tests:: --nocapture`, MEASUREMENT_RUNS = 7 + 1 warm-up,
harness-internal P50/P95) back-to-back on an otherwise idle machine; no
parallel measurement agents were active (unlike the O0 round, there was
nothing to serialize against — the shared measurement lock discipline targets
that condition). Baseline matrix: 52.5 s wall, head matrix directly after.

## 2. Results — predictor cost per fixture (harness P50, 7 reps)

| fixture | rows | baseline predictor P50 | head predictor P50 | reduction |
| --- | --- | --- | --- | --- |
| f3_wide_mixed | 10,000 | 13.07 ms | 4.23 ms | **67.6%** |
| f3_wide_mixed | 100,000 | 181.31 ms | 53.65 ms | **70.4%** |
| f6_project_heavy | 10,000 | 30.40 ms | 13.35 ms | **56.1%** |
| f6_project_heavy | 100,000 | 260.67 ms | 125.40 ms | **51.9%** |
| f7_filter_heavy | 10,000 | 5.93 ms | 1.70 ms | **71.3%** |
| f7_filter_heavy | 100,000 | 76.00 ms | 20.34 ms | **73.2%** |
| f8_derive_heavy | 10,000 | 7.79 ms | 3.61 ms | **53.7%** |
| f8_derive_heavy | 100,000 | 79.75 ms | 31.51 ms | **60.5%** |
| f5_rule_heavy | 10,000 | 6.52 ms | 2.13 ms | 67.3% |
| f5_rule_heavy | 100,000 | 85.85 ms | 25.74 ms | 70.0% |
| f4_long_utf8 | 10,000 | 1.67 ms | 0.51 ms | 69.5% |
| f4_long_utf8 | 50,000 | 7.02 ms | 2.74 ms | 61.0% |
| f1_narrow_fixed | 1,000,000 | 0.23 ms | 0.23 ms | neutral |
| f9_near_limit | 1,000,000 | 1.31 ms | 1.34 ms | neutral |

End-to-end effect where the predictor share was material: f3 100k
484.6 → 340.8 ms (29.7%), f6 100k 324.7 → 226.1 ms (30.4%), f7 100k
189.1 → 117.4 ms (37.9%). Fixed-width scenarios (f1/f2/f9) are unchanged
within noise — the refactor touches only the variable-width path.

Every variable-width scenario clears the O0-B1 §7 engine-E2E threshold
(P50 reduction ≥ 25% over ≥ 7 runs) by a wide margin, on the predictor cost
that the task targets.

## 3. Equality acceptance

- Full `stillflow-engine` lib suite green at the measured head (including the
  #292 neutrality/attribution tests and the f1–f10 measurement fixtures,
  which assert predicted byte-count invariants, final slices, and error
  surfaces).
- The refactor does not add allocations or caches: `WidthSource` borrows the
  array; no memory-accounting change is required (the "single-call exact
  width cache" second stage is NOT introduced — see §4).

## 4. Second stage (single-call width cache): not necessary, not introduced

Post-hoist, the remaining predictor cost on the worst scenario
(f6_project_heavy 100k, 56% share) is the Project-step full recompute
(`predict_step` deliberately recomputes every projected column's physical
size — the audited #292 attribution surface), not the widening scan. The
task boundary stands: byte-calculation caches and projection-index items
were downgraded by the #292 attribution evidence and are not optimized
item-by-item. If a future round wants that share, it is a separate,
separately-gated task.
