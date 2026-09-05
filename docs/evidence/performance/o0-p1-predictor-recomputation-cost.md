# O0-P1: Engine predictor recomputation cost measurement

- Version: 1.0.0
- Date: 2026-09-05
- Issue: #284 ([O0-P1])
- Measured head: `main@f61e0853b67ff5ca7bedb0bddb707befb922baff` (dispatch base; no
  production commits landed between base and measurement)
- Instrumentation branch: `agent/issue-284-o0-p1-predictor-cost`
- Reference baseline: O0-B1 (#282). Fixture E2E times here come from the
  instrumented test harness (scripted connector + tempdir snapshot store), not
  the #282 harness; absolute E2E should be cross-read against #282, while the
  predictor shares below are internally consistent within each run.
- Scope: measurement/instrumentation only. No production predictor algorithm,
  estimate, chunk boundary, resource ceiling, API, persistence format, or
  executor behavior is changed. The only production-path edits are
  measurement hooks that compile to zero-sized no-ops unless the
  `predict-metrics` cargo feature is enabled.

## 1. Summary

| Workload class | Predictor share of end-to-end runtime (P50, two runs) |
| --- | --- |
| Fixed-width plans (narrow/wide, incl. 2048 B literal derives) | 0.01% - 0.16% |
| Small input (100 rows) | ~1.6% - 2.1% (of a ~5-8 ms run) |
| Long UTF-8 (2 x 2048 B) | 12.9% - 14.1% |
| Derive/expression-heavy (8 x Utf8[32], 16 derives) | 27.2% - 30.0% |
| Rule-heavy (24 mutating rules) | 28.7% - 32.6% |
| Wide mixed variable-width (16 x Utf8[32]) | 32.1% - 35.9% |
| Filter-heavy (32 filters) | 35.0% - 44.3% |
| Project-heavy (64 cols -> 8) | 68.5% - 73.7% |

The predictor's cost is concentrated in exactly one term:
`refresh_source_widths` row-by-row scans of variable-width source columns,
which account for **98.3% - 99.9% of predictor wall time** on every
variable-width fixture (~92-151 ns per row scan, 120x-448x more rows scanned
than rows processed). All other predictor work (schema clones, per-rule and
Project full-column recomputations, export-transition byte math) is measured
at <= ~5% of predictor time and is immaterial end to end.

Ranking for **separate future implementation tasks**:

- **GO** - exact per-chunk variable-width scan reuse inside one
  `largest_feasible_k` call (two variants; eliminates the measured dominant
  term with bit-identical predictions).
- **NO-GO** - per-probe cached column byte calculations (contribution
  immaterial: the byte-math terms it would remove are <= 13% of a predictor
  share that is <= 0.05% on the fixtures where they are largest).
- **NO-GO** - projection ordinal/precomputed index maps (same evidence).
- **NO-GO** - cross-probe reuse of width measurements (cannot preserve the
  frozen exactness laws; see 7.5).
- **INCONCLUSIVE** - structural sharing / copy-on-write schema state (schema
  clones are a measured but small residual; only worth re-measuring after the
  GO item lands).

## 2. Instrumentation definition

### 2.1 Feature and code layout

- Cargo feature: `predict-metrics` on `stillflow-engine`
  (`backend/crates/stillflow-engine/Cargo.toml`). Disabled by default; no
  other crate or feature depends on it.
- Module: `backend/crates/stillflow-engine/src/predict_metrics.rs`. It
  provides one API surface with two compile-time implementations:
  - enabled: process-global `AtomicU64` counters (`Relaxed`) plus
    drop-guard `std::time::Instant` wall timers;
  - disabled: `#[inline(always)]` zero-sized no-ops with empty `Drop`, which
    the optimizer removes entirely. The disabled snapshot is the all-zero
    default.
- Hooks are placed in `predict.rs` (every counted call site), and at the two
  production call sites in `engine.rs` (`consume_envelope`, ingest site) and
  `preview.rs` (preview site) with chunk-loop wall timers. Test harness:
  `backend/crates/stillflow-engine/src/predict_metrics_tests.rs`.

### 2.2 Counters (snapshot fields, all `u64`)

- Calls/probes: `lfk_calls`, `predict_probes`; per-site
  `site_ingest_lfk_calls`, `site_preview_lfk_calls`.
- Wall timers (ns): `lfk_wall_ns`, `predict_wall_ns`,
  `site_ingest_lfk_wall_ns`, `site_preview_lfk_wall_ns`,
  `site_ingest_chunk_loop_wall_ns`, `site_preview_chunk_loop_wall_ns`, and
  sub-phase `refresh_source_widths_wall_ns`,
  `column_physical_sum_wall_ns`, `export_transition_wall_ns`.
- `PredictedSchema` clones by category: `clone_working_init` (predict entry),
  `clone_project`, `clone_filter`, `clone_rule` (one per rule application),
  plus derived `clone_total`.
- Width refresh: `refresh_source_widths_calls`,
  `source_columns_refreshed`.
- Variable/nested scans: `max_variable_width_calls`, `width_scan_rows` (rows
  examined row-by-row), `width_scan_value_bytes` (sum of value widths
  observed), `variable_data_bytes_calls`, `variable_data_rows`,
  `variable_data_span_bytes` (rows/bytes covered including O(1) offset-span
  reads), `list_scans`, `struct_scans`.
- Byte math: `column_physical_sum_calls`, `column_physical_sum_columns`,
  `column_physical_bytes_calls`.
- Project-induced full-column recomputations: `project_full_recomputes`
  (each is a `column_physical_sum` over the post-projection schema).
- Rule-induced full-column recomputations: `rule_full_recomputes` with
  per-kind `rule_recompute_{drop_column,trim,replace_literal,fill_null,cast}`
  plus `derive_temp_byte_calls` (DeriveColumn temporary byte computation) and
  `to_logical_schema_calls` (expression typing).
- Export-transition work: `export_transition_calls`,
  `export_transition_columns`, `export_transition_column_byte_calls`.

Attribution model: counters are cumulative process-global aggregates. Per-
`largest_feasible_k`-call attribution is exact because each measurement run
resets the counters, executes one serialized engine run, and reads one
snapshot; unit tests additionally bracket a single call and compare against a
replay-derived expectation (2.4). Error paths that `?` before a recording
point are not counted (fixtures do not trigger predictor errors). Counters
never influence predictor control flow; timers use `std::time::Instant` only
(predictor code is synchronous, so wall time approximates CPU time).

### 2.3 Output-neutrality evidence (acceptance criterion)

- `metrics_predict_outputs_stable_across_resets` (runs in BOTH feature
  modes): `largest_feasible_k`, `predict(k)`, and the single-row bound are
  identical across instrumentation resets, and `k` is verified to be the
  exact feasibility boundary (`predict(k) <= MAX_BATCH_BYTES <
  predict(k+1)`).
- `metrics_disabled_snapshot_stays_zero` (both modes): with the feature
  disabled the snapshot remains all-zero while the predictor executes; with
  the feature enabled it moves.
- The full `stillflow-engine` test suite (237 tests) passes unchanged in both
  modes; predicted values, `largest_feasible_k` outputs, chunking, and error
  categories are covered by the pre-existing expectation tests, which run
  identically with instrumentation compiled in or out.

### 2.4 Exact per-call attribution evidence (enabled mode)

- `metrics_enabled_fixed_width_probe_attribution_exact`: one
  `largest_feasible_k` call over 200k rows (1 x Int64 + 2048 B literal
  derive) produces exactly `1 + binary-search-iterations` probes (verified
  against a replay of the search), one working-init clone plus one rule clone
  per probe, one refresh per probe, one `column_physical_sum` per probe,
  6 `column_physical_bytes` calls per probe, and the export transition
  visiting 2 columns twice per probe. A second measured run reproduces the
  counter snapshot exactly.
- `metrics_enabled_variable_width_scan_attribution_exact`: single Utf8[32]
  column, 5k rows: `width_scan_rows` equals the sum of probe k values and
  `width_scan_value_bytes` equals `32 x width_scan_rows`;
  `variable_data_rows` equals `3 x` the scanned rows (initial sum + two
  export passes).
- `metrics_enabled_step_and_rule_categories_exact`: one `predict` with
  Project/Filter/six rules yields exact clone categories (1 working-init,
  1 project, 1 filter, 6 rule), `project_full_recomputes = 1`,
  `rule_full_recomputes = 5` with one per rule kind,
  `derive_temp_byte_calls = 1`, `to_logical_schema_calls = 1`, and the exact
  refresh/scan counts for the input schema.
- E2E counter determinism: for every fixture/row-count, all count fields
  (timers excluded) are identical across all 7 timed runs.

## 3. Machine, concurrency policy, and commands

- Machine: WSL2, 6 vCPU, 11 GB RAM (shared host).
- Concurrency policy: four sibling performance agents ran on the same host
  during measurement. All cargo commands shared
  `CARGO_TARGET_DIR=/home/owl/.cargo-o0-target` (builds serialize via
  cargo's lock). Every timed run was executed under
  `flock /tmp/stillflow-o0-measure.lock` so only one agent measures at a
  time; the remaining agents still consume CPU for builds/tests, which
  inflates cross-run wall-time noise (documented below).
- Each measurement point: 1 untimed warmup + 7 timed repetitions inside one
  process, reporting P50/P95. Two full runs of the whole matrix were executed
  per feature mode.
- Peak RSS: background sampler polling `/proc/<pid>/status` `VmHWM` of the
  test binary every 250 ms. Observed peak: 470 MB (enabled run), 453 MB
  (disabled run) - dominated by fixture envelope construction, far below the
  11 GB host limit. (`/usr/bin/time -v` is not installed on this host; the
  /proc sampler is equivalent for peak RSS.)

Reproduce (from `backend/`):

```bash
export CARGO_TARGET_DIR=/home/owl/.cargo-o0-target
# counters + timings + attribution (enabled)
flock /tmp/stillflow-o0-measure.lock -c \
  'cargo test -p stillflow-engine --lib --features predict-metrics \
   -- --test-threads=1 --nocapture metrics_e2e'
# neutral/no-counter baseline (disabled)
flock /tmp/stillflow-o0-measure.lock -c \
  'cargo test -p stillflow-engine --lib \
   -- --test-threads=1 --nocapture metrics_e2e'
# targeted neutrality/attribution tests (both modes)
cargo test -p stillflow-engine --lib -- metrics_disabled metrics_predict_outputs
cargo test -p stillflow-engine --lib --features predict-metrics -- \
  metrics_disabled metrics_predict_outputs metrics_enabled
```

Output protocol: one `PM_RUN` JSON line per timed repetition (e2e ms,
predictor ms, share, full counter snapshot) and one `PM_SUMMARY` JSON line
per fixture/row-count (P50/P95, spread, share P50).

Noise reporting: within a process, `e2e_spread_p95_vs_p50` was 0.4%-34%
(small fixtures are noisiest). Cross-run drift of P50 absolutes between the
two enabled runs was -19% to +36% - larger than any plausible instrumentation
effect - due to sibling agents sharing the host. Consequently:

- absolute E2E/predictor milliseconds in this note are indicative only;
- the **predictor share** (ratio measured within the same run) is the robust
  statistic and was stable to within ~1-2 percentage points across runs
  (e.g. project-heavy 73.7% vs 73.5% at 100k rows; wide-mixed 35.8% vs
  35.9%);
- counter fields are exactly deterministic and carry no noise.

## 4. Fixture matrix

All fixtures ingest through `ExecutionEngine::materialize_tracked` with a
scripted connector (envelopes <= 65,536 rows and <= 64 MiB each), a tempdir
`SnapshotStore`, and MAX_BATCH_BYTES = 64 MiB admission. `Utf8[w]` = w-byte
constant ASCII values.

| # | Fixture | Schema | Plan | Row counts | Envelope split |
| --- | --- | --- | --- | --- | --- |
| f1 | narrow fixed-width | 1 x Int64 | derive Utf8[64] literal | 10k / 100k / 1M | 65,536 rows |
| f2 | wide fixed-width | 64 x Int64 | none | 10k / 100k | 50k rows |
| f3 | wide mixed variable | 16 x Int64 + 16 x Utf8[32] | derive Utf8[32] literal | 10k / 100k | 50k rows |
| f4 | long UTF-8 | 2 x Utf8[2048] | none | 10k / 50k | 6k rows |
| f5 | rule-heavy | 8 x Utf8[32] + 8 x Int64 | 24 rules (Trim/ReplaceLiteral/FillNull per Utf8 col) | 10k / 100k | 50k rows |
| f6 | Project-heavy | 32 x Int64 + 32 x Utf8[16] | Project to 8 cols | 10k / 100k | 25k rows |
| f7 | Filter-heavy | 8 x Utf8[32] | 32 chained Filters | 10k / 100k | 50k rows |
| f8 | derive/expression-heavy | 8 x Utf8[32] | 16 x DeriveColumn(Cast-to-Utf8) | 10k / 100k | 50k rows |
| f9 | near-limit, many probes | 1 x Int64 | derive Utf8[2048] literal (k ~= 32k << remaining) | 100k / 500k / 1M | 65,536 rows |
| f10 | small input | 1 x Utf8[32] | derive Utf8[8] literal | 100 | 1 envelope |
| p | preview site | 1 x Utf8[32] | scan target | 2k | 1 envelope |

Two plan widths/rule counts are covered (1 rule vs 24/16 rules; 1/2 vs
64 columns); row counts vary by 10-100x within fixtures to expose scaling.
f9 maximizes probes per call (~15-17, mixed feasible/infeasible binary-search
probes verified at unit level by `metrics_enabled_fixed_width_probe_attribution_exact`)
while remaining memory-light. The preview smoke (`metrics_e2e_preview_site_smoke`)
exercises the `Site::Preview` path (`site_preview_lfk_calls >= 1`,
ingest site = 0).

## 5. Attribution results

### 5.1 Predictor share of end-to-end (P50 of 7 runs, two runs per point)

| Fixture | Rows | E2E P50 run1 (ms) | E2E P50 run2 (ms) | Predictor share run1 | run2 |
| --- | ---: | ---: | ---: | ---: | ---: |
| f1_narrow_fixed | 10k | 25.9 | 27.5 | 0.12% | 0.16% |
| f1_narrow_fixed | 100k | 253.3 | 248.4 | 0.03% | 0.04% |
| f1_narrow_fixed | 1M | 2927.4 | 2503.2 | 0.03% | 0.03% |
| f2_wide_fixed | 10k | 799.0 | 647.3 | 0.04% | 0.04% |
| f2_wide_fixed | 100k | 7377.0 | 6506.9 | 0.01% | 0.01% |
| f3_wide_mixed | 10k | 633.3 | 540.6 | 32.5% | 32.1% |
| f3_wide_mixed | 100k | 5870.3 | 6522.1 | 35.8% | 35.9% |
| f4_long_utf8 | 10k | 147.7 | 153.9 | 13.9% | 12.9% |
| f4_long_utf8 | 50k | 709.4 | 776.9 | 14.1% | 14.0% |
| f5_rule_heavy | 10k | 304.9 | 386.2 | 30.0% | 28.7% |
| f5_rule_heavy | 100k | 3183.5 | 3875.3 | 32.6% | 32.2% |
| f6_project_heavy | 10k | 561.8 | 600.0 | 70.4% | 68.5% |
| f6_project_heavy | 100k | 5063.4 | 6432.4 | 73.7% | 73.5% |
| f7_filter_heavy | 10k | 241.2 | 323.5 | 37.8% | 35.0% |
| f7_filter_heavy | 100k | 2261.0 | 2987.1 | 44.3% | 43.8% |
| f8_derive_heavy | 10k | 349.3 | 417.8 | 27.4% | 27.2% |
| f8_derive_heavy | 100k | 3742.5 | 4217.9 | 29.9% | 29.3% |
| f9_near_limit | 100k | 690.4 | 886.6 | 0.04% | 0.04% |
| f9_near_limit | 500k | 3582.3 | 4783.1 | 0.04% | 0.04% |
| f9_near_limit | 1M | 7317.2 | 9966.8 | 0.05% | 0.05% |
| f10_small | 100 | 4.8 | 6.5 | 1.8% | 1.6% |

Absolute end-to-end should be interpreted against the O0-B1 (#282) baseline;
shares are self-consistent within each run.

### 5.2 Where predictor time goes (enabled run 2, last repetition per point)

| Fixture | Rows | Probes | Chunks | Rows scanned | Scan bytes | refresh% | sum% | export% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| f1_narrow_fixed | 1M | 271 | 16 | 0 | 0 | 7.4% | 8.7% | 10.0% |
| f2_wide_fixed | 100k | 34 | 2 | 0 | 0 | 7.7% | 13.3% | 26.8% |
| f3_wide_mixed | 100k | 66 | 4 | 24,408,512 | 781 MB | 99.8% | 0.0% | 0.0% |
| f4_long_utf8 | 50k | 124 | 9 | 1,192,214 | 2.4 GB | 99.4% | 0.1% | 0.1% |
| f5_rule_heavy | 100k | 34 | 2 | 12,000,176 | 384 MB | 99.6% | 0.1% | 0.0% |
| f6_project_heavy | 100k | 64 | 4 | 44,801,280 | 717 MB | 99.9% | 0.0% | 0.0% |
| f7_filter_heavy | 100k | 34 | 2 | 12,000,176 | 384 MB | 99.8% | 0.0% | 0.0% |
| f8_derive_heavy | 100k | 66 | 4 | 12,262,288 | 392 MB | 94.5% | 0.0% | 0.0% |
| f9_near_limit | 1M | 1151 | 77 | 0 | 0 | 5.9% | 5.7% | 10.1% |

(Percentages are shares of `predict_wall_ns`; the remainder on rule fixtures
is schema cloning + rule bookkeeping + expression typing, e.g. f8's
non-refresh residual is ~5%.)

### 5.3 Repeated-work counters (per engine run, enabled run 1)

| Fixture | Rows | lfk calls | Probes | Scan rows | Amplification (scan rows / rows) | Rule full recomputes | Project full recomputes | `column_physical_bytes` calls | Export columns visited |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| f1 | 1M | 16 | 271 | 0 | 0x | 0 | 0 | 1,626 | 542 |
| f2 | 100k | 2 | 34 | 0 | 0x | 0 | 0 | 6,528 | 68 |
| f3 | 100k | 4 | 66 | 24,408,512 | 244x | 0 | 0 | 6,534 | 132 |
| f4 | 50k | 9 | 124 | 1,192,214 | 24x | 0 | 0 | 744 | 248 |
| f5 | 100k | 2 | 34 | 12,000,176 | 120x | 816 | 0 | 14,688 | 68 |
| f6 | 100k | 4 | 64 | 44,801,280 | 448x | 0 | 64 | 5,632 | 256 |
| f7 | 100k | 2 | 34 | 12,000,176 | 120x | 0 | 0 | 816 | 68 |
| f8 | 100k | 4 | 66 | 12,262,288 | 123x | 0 | 0 | 4,752 | 264 |
| f9 | 1M | 77 | 1,151 | 0 | 0x | 0 | 0 | 6,906 | 2,302 |

Cross-checks that validate the counters against the code structure: f5's 816
rule recomputes = 24 rules x 34 probes; f6's 64 project recomputes = 4 chunks
x 16 probes and `column_physical_sum_columns` = 4,608 = 4,096 (64-col initial
sums) + 512 (8-col post-projection sums); f8's `to_logical_schema_calls` =
1,056 = 16 derives x 66 probes; f4's `variable_data_rows` = 3 x scan rows
(initial sum + two export passes).

Per-row scan cost is consistent across fixtures: 92-151 ns per examined row
(`refresh_source_widths_wall_ns / width_scan_rows`), averaging ~105 ns. The
per-row loop performs a null check, a per-row `downcast_ref` to the concrete
string array, and a `value()+len()`.

### 5.4 Scaling observations

- Probe count per `largest_feasible_k` call is 1 + ceil(log2(remaining))
  (14-17 probes for chunk-sized remaining values; f9 totals 1,151 probes over
  77 calls at 1M rows) and is independent of column count.
- Predictor wall time scales linearly in rows x variable-columns x probes:
  f6 goes 0.41 s -> 4.71 s for 10x rows; f3 0.22 s -> 2.47 s.
- Predictor share of E2E is roughly stable or slightly increasing with row
  count on variable-width fixtures (f7: 37.8% -> 44.3%), so the predictor does
  not amortize at scale.
- Fixed-width predictor work is O(columns x probes) with zero row scans:
  0.68 ms at 1M rows (f1), 0.6 ms at 100k x 64 columns (f2).
- The export transition's double pass (2 x columns `column_physical_bytes`
  calls per probe) is visible in counters but contributes <= 27.8% of a
  predictor share that is <= 0.05% end to end on those fixtures.

## 6. Instrumentation overhead

- Disabled (default): hooks are zero-sized `#[inline(always)]` no-ops with
  empty `Drop`; there is no runtime artifact to measure. Behavior neutrality
  is proven by the targeted tests and the full suite passing unchanged.
- Enabled: per predictor run the instrumentation performs exactly the counted
  number of `Relaxed` atomic adds plus `Instant::now()` at five timer sites
  per probe. For the heaviest fixture (f6 at 100k rows) that is ~5,600 atomic
  adds and ~320 timer reads per engine run - analytically far below 0.1% of
  the measured 4.7 s predictor time.
- Empirically, paired enabled-vs-disabled E2E P50 deltas ranged -41% to +3.5%
  with no sign pattern (the disabled run was uniformly *slower* in the paired
  run, the opposite of an instrumentation cost). This is sibling-agent host
  drift, not an instrumentation effect; cross-run drift of the same mode
  (-19% to +36%) is the same order. The honest bound is: any instrumentation
  overhead is below the ~±35% cross-run noise floor of this shared host, and
  structurally it is the small atomic/timer cost quantified above.

## 7. Candidate reuse evaluation (analytical, not implemented)

Context from the measurements: the only material predictor term is the
per-probe rescanning of variable-width source rows in
`refresh_source_widths` (`max_variable_width`: per-row null check + per-row
downcast + `value()+len()`, ~105 ns/row), repeated by every binary-search
probe (14-17 per chunk) over the same window. Byte math, schema clones, and
the export transition are measured immaterial on every fixture where they
are relatively largest.

### 7.1 GO: exact per-chunk variable-width scan reuse (within one `largest_feasible_k` call)

Two exactness-preserving variants, in increasing ambition:

1. **Hoist the per-row downcasts** out of the scan loop (dispatch on the
   array type once per column, then scan a typed slice). Work eliminated: a
   large fraction of the ~105 ns/row constant; no behavioral surface touched.
   Retained memory: none. Invalidation key: none. Correctness risk: minimal
   (identical values computed; no caching at all). Conservative estimation:
   unchanged. Complexity/review: small; easily verified against the frozen
   byte-result law by the existing test suite.
2. **Per-chunk width cache + range-max**: within one `largest_feasible_k`
   call, materialize `value_width(row)` once per variable column over
   `offset..offset+remaining` into a `u32` buffer, then answer each probe's
   `max over offset..offset+k` from the cache (O(k) reads per probe), or from
   a segment tree (O(log k) per probe, 2x memory). Work eliminated: the
   measured dominant term - on f6-class plans ~105 ns x 448x rows of scanning
   drops to ~k reads + build, i.e. roughly two orders of magnitude on the
   predictor share (13-74% -> low single digits). Retained memory: per
   variable column per chunk window W <= 65,536 rows: 4W bytes for the cache
   (256 KiB/column) plus optionally 8W bytes for the tree (~768 KiB/column
   total for the tree variant; ~24 MiB transient for a 32-variable-column
   plan). This is new predictor scratch and MUST be added to the memory
   accounting in any future design (the issue forbids unaccounted caching).
   Invalidation key: none across calls - the cache is created and dropped
   inside a single `largest_feasible_k` call, so there is no cross-call
   lifetime or staleness surface at all. Correctness risk: low-moderate;
   `value_width(row)` is a pure function of immutable array content, so every
   probe observes an identical maximum and every predicted byte value,
   `largest_feasible_k` output, single-row bound failure, and chunk boundary
   is bit-identical. Conservative estimation: unchanged (no estimate is
   widened). Complexity/review: moderate (nested list/struct children and the
   `variable_data_bytes` fallback path need the same treatment or an explicit
   carve-out).

Justification under the issue's decision rule: the measured contribution is
material (up to 74% of E2E on Project-heavy variable-width plans, 13-44% on
the other variable-width fixtures), and variant 2's retained memory
(~0.25-0.75 MiB per variable column, transient, bounded by
MAX_BATCH_ROWS-scoped windows) and low correctness risk are justified by a
two-order-of-magnitude reduction of that term.

### 7.2 NO-GO: per-probe cached column byte calculations

Work eliminated: repeated `column_physical_bytes` calls inside
`column_physical_sum` and the export transition's double pass (2 x columns
calls per probe). Retained memory: O(columns) u64 per probe (trivial).
Invalidation key: (column id, origin, max_value_bytes, k, offset). Correctness
risk: low. Conservative estimation: unchanged (pure arithmetic memoization).
Why NO-GO: the measured share of `column_physical_sum` + export-transition
time is 0.0-0.1% of predictor time on all variable-width fixtures and <= 13%
of predictor time on fixed-width fixtures, where the predictor itself is
<= 0.05% of E2E (0.05-0.9 ms absolute). The counter is nonzero but the
end-to-end contribution is immaterial - exactly the case the issue's decision
rule excludes.

### 7.3 NO-GO: projection index / precomputed ordinal maps

Work eliminated: `columns.contains`/`position` scans and the sort in the
Project step, plus repeated position lookups. Retained memory: an ordinal map
per Project step (columns x 4 B) if hoisted to the prepared plan (invalidation
key: plan identity), or O(k) per probe if rebuilt. Correctness risk: low.
Conservative estimation: unchanged. Why NO-GO: Project-side arithmetic is
contained in the measured sum/export/clone residuals, which are <= ~5% of
predictor time even on the Project-heavy fixture, i.e. << 1% of E2E; f6's 64
project recomputes per 100k-row run are dominated by the projection's row
scans, not its index arithmetic. Revisit only for very wide plans (1000+
columns, cf. the 4,096-column test) where O(n*m) ordinal scans could grow.

### 7.4 INCONCLUSIVE: structural sharing / lighter schema state instead of whole-schema clones

Work eliminated: `clone_rule`/`clone_project`/`clone_filter`/`clone_working_init`
deep clones (each clones every `PredictedColumn`, including the `String`
name and `LogicalType`). Measured: f8 at 100k rows performs 1,122 clones per
engine run; clones plus rule bookkeeping plus expression typing are the ~5%
non-refresh residual of predictor time on rule-heavy fixtures (f5: 850 sums
and 0 rule recomputes beyond that; the residual is the clones themselves).
Retained memory: `Arc` header per shared column vector + refcount traffic;
roughly neutral at rest, slightly positive under COW mutation. Invalidation
key: any column mutation (origin, name, type, nullability, max width) must
trigger copy-on-write. Correctness risk: moderate (aliasing/mutation bugs),
higher than 7.1. Conservative estimation: unchanged if COW is exact. Why
INCONCLUSIVE: the current measured contribution (~5% of predictor time, and
predictor time is 27-33% of E2E on those fixtures, so ~1-2% of E2E) does not
clear the materiality bar on its own, but it becomes the largest residual
after 7.1 lands; re-measure with the same instrumentation before deciding.

### 7.5 NO-GO: cross-probe reuse of width measurements (or predictions)

The binary search never repeats a probe (mids are strictly monotone; the
mandatory single-row probe is never re-probed), so exact memoization across
probes has a 0% hit rate. The only reuse that hits is *inexact*: substituting
a superset maximum (e.g. the whole-window max) for a sub-window probe. That
changes the predicted byte result for the probed `k`, can lower
`largest_feasible_k`, and therefore changes chunk boundaries - violating the
frozen safety laws (exact predicted bytes per `(k, offset, arrays, schema,
steps)`, identical `largest_feasible_k`, identical chunk boundaries). Any
future proposal in this direction must preserve exactness to be admissible;
as proposed, it is rejected. The GO candidate in 7.1 is the exactness-
preserving form of this idea.

## 8. Memory-budget note

No caching was implemented. For the GO candidate, the required memory
accounting entry is: predictor scratch of 4W bytes per variable-width source
column (plus 8W bytes if a segment tree is used) per in-flight
`largest_feasible_k` call, W <= remaining rows of the current envelope
(<= MAX_BATCH_ROWS = 65,536), i.e. <= ~24 MiB transient for a
32-variable-column plan at the largest measured window. This must be counted
in the engine's live/peak budget in any future implementation, and the
single-row bound behavior is unaffected because the cache is allocated after
the single-row probe passes and its failure path never touches the cache.

## 9. Final ranking

| Candidate | Work eliminated (measured) | Retained memory | Exactness | Verdict |
| --- | --- | --- | --- | --- |
| Per-chunk variable-width scan reuse (7.1) | 98-99% of predictor time on variable-width plans (13-74% of E2E) | 4W-12W B per variable column, call-scoped | Bit-identical by construction | **GO** |
| Per-probe column byte-calc cache (7.2) | <= 13% of a <= 0.05% predictor share | O(columns)/probe | Exact | **NO-GO** (immaterial) |
| Projection ordinal maps (7.3) | < 1% of E2E | O(columns)/step | Exact | **NO-GO** (immaterial) |
| Schema structural sharing / COW (7.4) | ~1-2% of E2E on rule-heavy plans | Arc/COW headers | Exact if COW exact | **INCONCLUSIVE** (re-measure after 7.1) |
| Cross-probe width/prediction reuse (7.5) | n/a (0% exact hit rate) | n/a | Violates frozen laws | **NO-GO** |

For fixed-width-only plans no predictor optimization is warranted at any
level (share <= 0.16%).

## 10. Acceptance criteria checklist

- [x] Instrumentation is disabled by default and behavior-neutral.
      (`predict-metrics` off by default; zero-sized no-op hooks; targeted
      tests in both modes; full suite green in both modes.)
- [x] Repeated work is counted by category, not inferred from source alone.
      (42 counters/timers; exact replay-verified attribution tests; counters
      validated against code-structure arithmetic in 5.3.)
- [x] At least fixed-width, wide-variable, long-string, rule-heavy and
      project-heavy cases are measured. (f1, f2, f3, f4, f5, f6 plus f7-f10
      and preview.)
- [x] Predictor time is reported both absolutely and as a fraction of
      relevant end-to-end work. (5.1, 5.2.)
- [x] Measurement noise is reported. (In-process P50/P95 spread, cross-run
      drift, paired-mode comparison, and the concurrency policy in 3/6.)
- [x] Candidate cache/reuse memory is included in the resource-budget
      analysis. (Section 8.)
- [x] No production predictor algorithm, estimate, chunk boundary, resource
      ceiling, API, persistence format, or executor architecture changes.
      (Hooks only; `git diff` limited to predict.rs hooks, two call-site
      timers, the new metrics module, tests, and the feature flag.)
- [x] Targeted tests demonstrate instrumentation does not alter predicted
      values or `largest_feasible_k` outputs. (2.3/2.4.)

## 11. Deviations and limitations

- `/usr/bin/time -v` is unavailable on this host; peak RSS was sampled from
  `/proc/<pid>/status` (`VmHWM`) at 250 ms granularity instead.
- E2E fixtures use a scripted in-process connector and a tempdir snapshot
  store, so absolute E2E times are harness-specific; the O0-B1 (#282) baseline
  remains the reference for absolute end-to-end significance. Predictor
  shares are measured within the same run and are the robust statistic.
- The preview path was smoke-measured (site counters light up) rather than
  run through the full fixture matrix; the predictor code path is shared with
  ingest, so the attribution findings carry over.
- Nested List/Struct fixtures were exercised only at the unit level (scan
  counters exist and are wired through `list_scans`/`struct_scans`); the
  measured matrix covers the Utf8 and fixed-width cases the issue requires.
- Sibling agents shared the host during measurement; wall-time absolutes
  carry that noise and are reported as such (sections 3 and 6).
