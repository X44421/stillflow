# O0-C1 — CSV decode/validation duplicate work on post-H3 main (Issue #285)

Measurement/instrumentation only. No production parser, validation, schema,
API, dependency (version) or resource-limit change; no CSV parser replacement;
no production fusion. This note quantifies — **by counting, not by code
inspection** — the duplicate logical work performed by the local-tabular CSV/TSV
path (Polars decode + lockstep `csv`-crate row validation) on the exact
dispatched head, distinguishes full from bounded/prefix reads, and evaluates
candidate future designs analytically.

- **Exact measured head:** `main@f61e0853b67ff5ca7bedb0bddb707befb922baff`
  (= dispatched base = `origin/main` at dispatch; branch
  `agent/issue-285-o0-c1-csv-dup-work`, working tree clean at f61e085 before
  the instrumentation commit).
- **Raw machine-readable records:** [`o0-c1-records.jsonl`](./o0-c1-records.jsonl)
  (fixture identities, one record per case and per parity run; counter
  percentiles across reps).
- **Reference baselines:** PR #293 (O0-B1, issue #282) measured this same head;
  the E24-B2BASE counters/fixture generators (issue #99) are reused verbatim.

## 1. Environment and concurrency policy

| Item | Value |
| --- | --- |
| CPU | 12th Gen Intel(R) Core(TM) i3-12100F, 6 vCPU (`nproc`), WSL2 kernel `6.18.33.2-microsoft-standard-WSL2` |
| RAM | 11 GiB total (shared with sibling conversations) |
| Toolchain | rustc/cargo 1.85.0 (pinned in `rust-toolchain.toml`) |
| Build profile | `cargo test --release`; the repo defines no `[profile.release]` override; the `[profile.test]` `debug = 0` override applies only to dev-profile builds |
| Shared target dir | `CARGO_TARGET_DIR=/home/owl/.cargo-o0-target` (sibling conversations share build artifacts; cargo's file lock serializes builds) |
| Concurrency policy | Other agent conversations ran in parallel on sibling worktrees. **Every timed measurement run was executed inside `flock /tmp/stillflow-o0-measure.lock`**, so only one task measured at a time. Untimed work (fixture generation, coding, building, parity-free smoke checks) ran outside the lock. |
| Known environment limitations (disclosed) | `/usr/bin/time` unavailable in this environment → peak RSS via `/proc` VmHWM (process-lifetime, includes warm-up and witness runs; not per-run). CPU ticks quantized at 10 ms. `Instant::now()` timer pairs inside the connector run on the consumer thread only (see §4.2 for the decode-overlap caveat). |

## 2. What the production CSV/TSV path does on this head (measured structure)

One `prepare_reader` call opens the same file **three times** (inspection, Polars
decoder handle, `csv`-crate validator handle — historical O0-D0 C1 structure,
unchanged). During streaming, per Polars decode step, `fill_pending` calls
`decoder.next_batches(1)` and then `validate_rows(frame.height())`, so the
`csv::Reader` validator walks the same rows in lockstep and re-reads the same
bytes through its own handle. Schema inference (inspect stage) reads a bounded
prefix (`schemaInference.maxBytes = 1 MiB` in this campaign's connection
config) and samples at most `maxRows = 100` rows.

Bounded reads are real production surface: `preview` passes
`max_rows = Some(row_limit + 1)` into the same reader, so the Polars decoder
stops at the limit and the validator follows in lockstep. `read_batches` always
runs unbounded (`max_rows: None`).

## 3. Instrumentation (measurement-only, disabled by default)

Private cargo feature `io-metrics` (existing, accepted E24-B2BASE mechanism)
extended with O0-C1 counters. All additions are `#[cfg(feature = "io-metrics")]`
and the un-instrumented code path is statement-for-statement identical to
before; with the feature off (the default) the module does not exist. The
feature adds:

- `csv_rows_decoded` — row heights of the Polars-decoded frames (per batch).
- `csv_fail_decode` / `csv_fail_validate` — which stage raised the terminating
  error, recorded **after** the error was produced (no error-timing change).
- `ingest_inspect_nanos`, `ingest_prepare_nanos`, `ingest_decode_nanos`,
  `ingest_validate_nanos` — wall-time accumulators around the four stages
  (inspect = bounded inference; prepare = delimited validator build + header
  re-check + Polars batched-reader construction; decode = consumer-side
  blocking time in `next_batches(1)`; validate = `validate_rows`).
- Pre-existing E24 labels kept verbatim: `validator_read_bytes` (exact logical
  bytes through the validator handle via `CountingReader`),
  `inference_phase_bytes` (exact logical inspect bytes), `decoder_os_bytes`
  (OS-level full-file-size observation; Polars may mmap, so **not** exact
  logical read bytes), `csv_decoder_invocations`, `csv_rows_validated`.

Counters are relaxed atomics; timers are two `Instant` calls per batch; nothing
allocates on the hot path. The dump side channel (`E24_IO_METRICS_OUT`,
historical name) writes `label=value` lines once per reader drop.

**Decode-timing caveat (disclosed):** `ingest_decode_nanos` measures the
consumer thread's blocking time in `next_batches(1)`; Polars may decode ahead
on its own threads, so this is a blocking-time observation, not exclusive CPU
time. Validation runs on the consumer thread, so `ingest_validate_nanos` is
exclusive wall time. Both are reported alongside harness-level total wall and
process CPU.

### 3.1 Harness

`tests/o0_c1_csv_dup_work.rs` (new, `#[ignore]`, one case per process via
`O0_C1_CASE`, `O0_C1_MODE=generate|measure`). It compiles **with and without**
`io-metrics`; with the feature off it still runs every case so digests and
error witnesses can be compared (behavioral parity witness, §8). Modes:

- `full` — production `read_batches` drain to the end.
- `bounded-preview` — production bounded read via `PreviewRequest`
  (`row_limit = 10_000`, `byte_limit = 50 MiB`); only a prefix is consumed.
- `bounded-earlydrop` — consumer-driven prefix consumption: take 3 envelopes
  (3 × 4 096 rows) then drop the stream.
- `validate-probe` — harness-side reference probe: one plain `csv`-crate pass
  over the fixture with the validator's settings (delimiter, quote, headers,
  `flexible(false)`), no type checks. **Not** the production path; it isolates
  the marginal cost of one text re-parse pass for comparison.

## 4. Fixture matrix (deterministic; SHA-256 recorded per record)

| Fixture | Shape | Bytes | SHA-256 (first 16) | Class |
| --- | --- | ---: | --- | --- |
| `anchor-csv-10c-100k` | 10 cols × 100 000 rows | 65 000 115 | `1a93d7f2f3ec3335` | E24-B2BASE anchor generator verbatim (identical SHA to PR #293's `ingest-csv-anchor-10c-100k` fixture) |
| `anchor-csv-10c-1m` | 10 cols × 1 000 000 rows | 650 000 090 | `07b046503f260a1e` | anchor at scale |
| `anchor-tsv-10c-100k` | 10 cols × 100 000 rows, tab-separated | 65 000 115 | `d2af71e94222b4b3` | TSV anchor (same cells as CSV anchor) |
| `narrow-fixed-8c-100k` | 8 cols × 100 000 rows (Int64/Float64/Utf8 mix) | 10 200 024 | `367b1e2910af8a49` | narrow fixed-width |
| `narrow-fixed-tsv-8c-100k` | same, tab-separated | 10 200 024 | `e3d33779b3270a01` | TSV narrow |
| `wide-mixed-128c-100k` | 128 cols × 100 000 rows (Int/Float/Utf8 mix) | 108 393 184 | `173dc23df39b1132` | wide mixed |
| `longutf8-8c-100k` | 8 cols × 100 000 rows, 32–2 048 B quoted payloads | 109 990 716 | `b4733c8c8f0e5f57` | long UTF-8 variable width |
| `malformed-width-10c-60k` | 10 cols × 60 000 rows, one 3-field row at row 40 000 | 6 359 938 | `4509e06408ee1d07` | malformed: width drift |
| `malformed-typed-8c-60k` | 8 cols × 60 000 rows, all-Int64, one 10-char non-integer cell at row 40 000 (width preserved) | 5 280 024 | `78551655a4c0e8de` | malformed: typed schema drift |

The narrow/wide/long-UTF-8/width-drift generators are the O0-B1 (PR #293)
fixture generators (cell-identical), the anchor cells are the E24-B2BASE
generator verbatim; TSV variants reuse the same cell payloads with tab
separators; `malformed-typed` is new for the typed-drift failure-phase question.
All fixture bytes and digests are in `o0-c1-records.jsonl`.

## 5. Measurement commands (exact)

```bash
export CARGO_TARGET_DIR=/home/owl/.cargo-o0-target
cd backend
cargo test --release -p stillflow-connector-local-tabular \
  --features io-metrics --test o0_c1_csv_dup_work --no-run
BIN_ON=$(ls -t "$CARGO_TARGET_DIR"/release/deps/o0_c1_csv_dup_work-* | grep -v '\.d$' | head -1)
# (the feature-off binary is built the same way without --features: BIN_OFF)

# fixture generation (untimed, outside the lock):
O0_C1_CASE=full-csv-anchor-10c-100k O0_C1_MODE=generate \
  O0_C1_FIXTURE_ROOT=/tmp/o0-c1-fixtures "$BIN_ON" \
  --exact --ignored --nocapture o0_c1_csv_dup_work

# timed run (serialized with the shared measurement lock; one case per process):
flock /tmp/stillflow-o0-measure.lock -c \
  'O0_C1_CASE=full-csv-anchor-10c-100k O0_C1_MODE=measure \
   O0_C1_FIXTURE_ROOT=/tmp/o0-c1-fixtures \
   O0_C1_HEAD=f61e0853b67ff5ca7bedb0bddb707befb922baff \
   "$BIN_ON" --exact --ignored --nocapture o0_c1_csv_dup_work'
```

Every case ran 1 untimed warm-up + 5–7 timed reps + 1 untimed witness run, one
process per case, all timed reps inside the flock. Parity cases re-ran the same
case with the feature-off binary.

## 6. Stage attribution — full reads (the duplicate-work measurement)

All phase times are P50 across reps of the per-rep counter deltas
(`counters_p50_across_reps` in the records); wall P50/P95 from 5–7 timed reps.
`validator_read_bytes` and rows are exact logical counts (CountingReader /
record heights), and they were **bit-stable across reps** (P50 = P95 per case).

| Case | Wall P50 (P95) ms | Inspect ms | Prepare ms | Decode ms | **Validate ms** | **Validate / wall** | **Validate / decode** | Decode invocations | Rows decoded = validated |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| full-csv-anchor-10c-100k | 360 (394) | 1.1 | 0.08 | 147.9 | **123.4** | **34.3 %** | 0.83 | 99 | 100 000 = 100 000 |
| full-tsv-anchor-10c-100k | 323 (328) | 1.1 | 0.06 | 137.5 | **101.5** | **31.4 %** | 0.74 | 99 | 100 000 = 100 000 |
| full-csv-narrow-fixed-8c-100k | 93 (99) | 0.7 | 0.07 | 44.6 | **29.4** | **31.6 %** | 0.66 | 99 | 100 000 = 100 000 |
| full-tsv-narrow-fixed-8c-100k | 115 (136) | 0.8 | 0.07 | 50.7 | **40.6** | **35.3 %** | 0.80 | 99 | 100 000 = 100 000 |
| full-csv-wide-mixed-128c-100k | 1 009 (1 033) | 1.4 | 0.15 | 470.7 | **330.0** | **32.7 %** | 0.70 | 99 | 100 000 = 100 000 |
| full-csv-longutf8-8c-100k | 533 (575) | 1.0 | 0.08 | 322.8 | **141.1** | **26.5 %** | 0.44 | 99 | 100 000 = 100 000 |
| full-csv-anchor-10c-1m | 2 388 (2 794) | 0.5 | 0.08 | 1 148.2 | **860.6** | **36.0 %** | 0.75 | 99 | 1 000 000 = 1 000 000 |

CPU P50 ≈ wall P50 for every case (e.g. anchor 360 vs 360 ms; 1 m 2 390 vs
2 388 ms): the drain is single-thread-bound on the consumer side; Polars
decode-ahead does not overlap the lockstep validation away at these shapes.

### 6.1 Byte attribution (logical, counted)

| Case | Fixture bytes | `validator_read_bytes` (P50 = P95) | **Validator / file** | `inference_phase_bytes` | `decoder_os_bytes` |
| --- | ---: | ---: | ---: | ---: | ---: |
| full-csv-anchor-10c-100k | 65 000 115 | 65 000 115 | **100.00 %** | 1 048 576 | 65 000 115 (OS-level) |
| full-tsv-anchor-10c-100k | 65 000 115 | 65 000 115 | **100.00 %** | 1 048 576 | 65 000 115 (OS-level) |
| full-csv-narrow-fixed-8c-100k | 10 200 024 | 10 200 024 | **100.00 %** | 1 048 576 | 10 200 024 (OS-level) |
| full-tsv-narrow-fixed-8c-100k | 10 200 024 | 10 200 024 | **100.00 %** | 1 048 576 | 10 200 024 (OS-level) |
| full-csv-wide-mixed-128c-100k | 108 393 184 | 108 393 184 | **100.00 %** | 1 048 576 | 108 393 184 (OS-level) |
| full-csv-longutf8-8c-100k | 109 990 716 | 109 990 716 | **100.00 %** | 1 048 576 | 109 990 716 (OS-level) |
| full-csv-anchor-10c-1m | 650 000 090 | 650 000 090 | **100.00 %** | 1 048 576 | 650 000 090 (OS-level) |

**Counted conclusion (full read):** the lockstep `csv`-crate validator re-reads
**exactly 100 % of the file's logical bytes** and validates **exactly the rows
Polars decoded** (ratio rows_validated / rows_decoded = 1.000 in every case,
byte-for-byte and row-for-row, not inferred). Inspection adds one bounded
1 MiB inference pass (1.6 % of the 65 MB anchor, 0.16 % of the 1 M-row file).
The decoder byte count is an OS-level label only (mmap), not exact logical I/O.

### 6.2 Where the validation time itself goes (probe cross-check)

| Reference | P50 wall | Interpretation |
| --- | ---: | --- |
| validate-probe, anchor 10c (all-Utf8 cells) | 103 ms | one plain csv-crate full-file pass, no type checks |
| in-connector `ingest_validate_nanos`, same fixture | 123.4 ms | lockstep validation ≈ standalone pass + per-batch handoff |
| validate-probe, wide 128c (numeric-heavy) | 196 ms | one plain csv-crate full-file pass |
| in-connector `ingest_validate_nanos`, wide 128c | 330.0 ms | + per-cell type re-verification (`csv_value_matches`) ≈ 134 ms ≈ 41 % of the validation pass |

For numeric-heavy schemas the per-cell type re-verification is roughly half of
the validation pass; for all-Utf8 payloads it is negligible. Both sub-passes
(parse + type-check) are pure re-derivation of facts the decode stage also
computes with an explicit schema.

## 7. Full vs bounded reads — the prefix distinction (required by the issue)

Bounded reads consume a strict prefix; the duplicate work follows the
**consumed prefix**, not the file:

| Case | Consumed | `validator_read_bytes` | **Validator / file** | Prefix share of rows | Rows decoded = validated |
| --- | --- | ---: | ---: | ---: | ---: |
| bounded-preview-csv-narrow-fixed-8c-limit10k | 10 000-row preview (limit 10 000 + 1 look-ahead) | 1 024 000 | **10.04 %** | 10 001/100 000 = 10.001 % | 10 001 = 10 001 |
| bounded-preview-tsv-narrow-fixed-8c-limit10k | same | 1 024 000 | **10.04 %** | 10.001 % | 10 001 = 10 001 |
| bounded-preview-csv-longutf8-8c-limit10k | 10 000-row preview | 10 993 664 | **10.00 %** | 10.001 % | 10 001 = 10 001 |
| bounded-earlydrop-csv-anchor-10c-3batches | 3 × 4 096-row envelopes (12 288 emitted) | 8 126 464 | **12.50 %** | 12 492/100 000 = 12.49 % | 12 492 = 12 492 |

- The validator validates exactly the rows Polars decoded in every bounded
  case (rows validated / rows decoded = 1.000), and its byte share of the file
  (10.00–12.50 %) matches the consumed row share (10.001–12.49 %): duplication
  is per-consumed-byte, so full reads pay 2× logical parsing and bounded reads
  pay 2× on their prefix only. Evidence therefore cannot overstate bounded-read
  costs. (Decoder-side bytes are OS-level labels only, so the byte ratio to the
  decoder's exact logical consumption is inferred from the exact row lockstep,
  not directly counted.)
- Bounded walls: preview cases 12–80 ms P50 (P95 16–93), early-drop 45 ms P50
  (P95 49); peak RSS 19–115 MiB (vs 90 MiB / 666 MiB for the full 100 k / 1 M
  anchors). Inspect (0.6–1.1 ms) and prepare (≈0.1 ms) are negligible in
  absolute terms in both regimes (up to 6.7 % of wall on the smallest preview
  case); preview `decode/wall` 42.6–56.6 %, `validate/wall` 20.0–23.0 %.
- Rows decoded (12 492 / 10 001) slightly exceed emitted rows (12 288 / 10 000)
  because the pipeline decodes/validates one partial look-ahead chunk beyond
  the emission cut — the validator never reads beyond what decode consumed.

## 8. Malformed / schema-drift failure phase and category (counted)

The terminating stage is counted by `csv_fail_decode` / `csv_fail_validate`
(incremented after the error was produced — no timing or behavior change):

| Case | Failing stage (counted) | Category | Error witness (stable across reps and features) | Progress at failure |
| --- | --- | --- | --- | --- |
| full-csv-malformed-width-10c-60k | **decode** (`csv_fail_decode=1`, `csv_fail_validate=0`) | `SchemaDrift`, retryable=false | "source data is malformed or incompatible with the established schema" | 39 936 rows decoded = validated (66.59 % of file bytes); 36 864 rows emitted |
| full-csv-malformed-typed-8c-60k | **decode** (`csv_fail_decode=1`, `csv_fail_validate=0`) | `SchemaDrift`, retryable=false | identical witness | 40 000 rows decoded = validated (66.71 % of bytes); 36 864 emitted |

Findings:

- On this head, for both drift classes and these fixtures, **the Polars decode
  stage raises the terminating error first**; the lockstep validator (which
  rejects the same row classes with row-attributed `InvalidData`/`SchemaDrift`
  errors when it reaches them) never reaches the defect because decode fails in
  the same chunk. Which stage terminates is fixture- and option-dependent
  (decode-ahead, chunk boundaries, per-type accept sets) — exactly why it had
  to be counted rather than inferred.
- Lockstep held until failure: rows decoded = rows validated at every prefix
  (39 936 / 40 000); the validator had consumed 66.6 % of the file.
- Emitted rows (36 864 = 9 × 4 096) < decoded rows (39 936) because of the
  batched pipeline's decode-ahead; failure timing is at batch boundaries, and
  the error taxonomy (category, retryable, sanitized message) is identical
  with and without instrumentation.

## 9. P50/P95, noise, peak RSS, allocations

| Case | Wall P50/P95 (min–max) ms | Reps | CPU P50 ms | Peak RSS MiB (VmHWM, process-lifetime) | Alloc bytes (case-level) |
| --- | --- | ---: | ---: | ---: | ---: |
| full-csv-anchor-10c-100k | 360/394 (297–394) | 7 | 360 | 88 | 4.2 GB |
| full-tsv-anchor-10c-100k | 323/328 (307–328) | 7 | 330 | 88 | 4.2 GB |
| full-csv-narrow-fixed-8c-100k | 93/99 (82–99) | 7 | 100 | 27 | 605 MB |
| full-tsv-narrow-fixed-8c-100k | 115/136 (82–136) | 7 | 130 | 27 | 605 MB |
| full-csv-wide-mixed-128c-100k | 1 009/1 033 (991–1 033) | 5 | 1 010 | 148 | 5.2 GB |
| full-csv-longutf8-8c-100k | 533/575 (511–575) | 7 | 540 | 139 | 5.9 GB |
| full-csv-anchor-10c-1m | 2 388/2 794 (2 286–2 794) | 5 | 2 390 | 666 | 19.8 GB |
| full-csv-malformed-width-10c-60k | 51/64 (45–64) | 7 | 60 | 23 | 392 MB |
| full-csv-malformed-typed-8c-60k | 40/42 (34–42) | 7 | 40 | 19 | 194 MB |
| bounded-preview-* (3 cases) | 12–80 / 16–93 | 7 each | 10–80 | 19–115 | 62 MB–660 MB |
| bounded-earlydrop-csv-anchor-10c-3batches | 45/49 (41–49) | 7 | 50 | 69 | 531 MB |
| probe-csv-validate-anchor-10c-100k | 103/133 (95–133) | 7 | 100 | 69 | 119 MB |
| probe-csv-validate-wide-mixed-128c-100k | 196/197 (173–197) | 5 | 190 | 110 | 124 MB |

Noise observations:

- Within one campaign, P95/P50 spans 1.01 (probe wide) … 1.33
  (bounded-preview-tsv); sub-second WSL2 cases are noisy, ≥1 s cases are tight
  (1.02–1.17).
- Absolute walls vary between campaigns run hours apart under sibling load
  (anchor 100 k: P50 253 ms in a first campaign, 360 ms in the final one; the
  feature-off parity runs in the final campaign measured 250 ms for the same
  case). **Ratio-based conclusions (byte shares, row lockstep, stage shares)
  are internal to each run and were identical in both campaigns**; absolute
  walls should be compared only within a single campaign.
- Cross-reference: PR #293 (O0-B1) measured the same anchor fixture (SHA
  `1a93d7f2…`) at P50 381 ms on the same head — consistent with the
  observed cross-campaign spread.
- Peak RSS is process-lifetime VmHWM (warm-up + reps + witness), disclosed as
  not per-run; allocations are case-level counting-allocator deltas.

## 10. Correctness witnesses

1. **Digest parity, instrumentation on vs off (the central witness).** The
   harness compiles and runs with the feature disabled; emitted-batch SHA-256
   digests (row order, types, null counts, buffers) and preview/early-drop
   digests are **identical** between feature-on and feature-off runs for:
   `full-csv-anchor-10c-100k` (`836cd9ab…`), `full-csv-narrow-fixed-8c-100k`
   (`165ce753…`), `bounded-preview-csv-narrow-fixed-8c-limit10k`
   (`dc3fe2e5…`), and both malformed cases (identical error witnesses, 36 864
   rows emitted before the terminal error in both modes).
2. **Accepted/rejected behavior unchanged:** same rows accepted (digests
   above), same terminal category (`SchemaDrift`), same sanitized message, same
   retryable flag, on and off.
3. **Cross-format invariance:** CSV and TSV anchors with identical cell
   payloads produce identical digests (`836cd9ab…`, `165ce753…`) — the
   duplicate-work structure is delimiter-independent.
4. **Stability:** row counts stable across all reps of every case
   (`rows_per_run_stable`), error witnesses stable across reps.
5. **Targeted tests pass with instrumentation on and off:**
   `cargo test -p stillflow-connector-local-tabular` → 23 unit + 26
   (`direct_projected_writer`) + 11 (`local_tabular`) + 1 (`memory_bound`)
   pass, evidence tests ignored as designed; identical results with
   `--features io-metrics`. `cargo fmt --all -- --check` green.
6. **Instrumentation overhead:** feature-on vs feature-off wall P50 deltas are
   within noise (253 vs 250, 62 vs 62, 29 vs 25, 30 vs 30, 7 vs 8 ms across the
   two campaigns' parity cases).

## 11. Candidate future designs (analytical only — nothing implemented)

Measured context for all candidates: on full reads the second pass costs
26.5–36.0 % of ingestion wall (P50) and re-parses 100 % of consumed bytes; the
pass decomposes into text re-parse (≈ probe cost: 103 ms/65 MB anchor;
196 ms/108 MB wide) plus per-cell type re-verification (≈ 0–41 % of the pass,
dtype-dependent); inspect and prepare stages are negligible in absolute terms
(≤ 1.4 ms, ≤ 1.5 % of full-read walls, ≤ 7 % on the smallest preview cases).

### 11.1 Fuse validation into decode-time traversal

- **Semantic risk: high.** Validation today is a second, independent parser
  (`csv` crate) whose accept/reject boundary is part of the established
  taxonomy (quoting, ragged lines, flexible-width rejection). Polars' parser
  accepts/rejects different edge cases (measured in §8: Polars raises first
  for both drift classes). Fusion makes one parser govern both concerns;
  per-byte acceptance would change for inputs where the parsers disagree.
- **Error-timing risk: high.** Failures currently surface at batch boundaries
  with stage-specific categories; the fusing stage's accept set decides which
  errors exist at all. Row-attributed validator errors
  ("… at row N") would have to be reproduced from decode-internal positions.
- **Expected memory effect:** neutral to slightly positive — removes the
  lockstep `StringRecord` scratch; Polars already materializes typed columns,
  `low_memory` is already on. Peak RSS is decode-dominated (§9).
- **Required correctness oracle:** differential oracle over an adversarial
  corpus (quoting/escaping, embedded separators, ragged rows, CRLF/BOM/NUL,
  invalid UTF-8, huge fields, `missing_is_null` empties) proving identical
  accepted/rejected row sets, identical category/retryable/message/row for
  every injected defect, identical emitted digests, and invariance across
  batch sizes; plus a statement of which parser governs each edge case.

### 11.2 Reuse parser-produced row metadata

- **Semantic risk: medium.** Keep two logical stages but let the validator
  consume decode-produced metadata (field counts/positions/typed cells)
  instead of re-parsing text. Residual risk concentrates where text-level
  checks and decoded-value checks disagree (text `"007"` vs Int64 `7`;
  `try_parse_dates` vs chrono format accept sets; `missing_is_null`).
- **Error-timing risk: medium.** Lockstep order is preserved by construction,
  but the failing stage/category can flip on the disagreement set above.
- **Expected memory effect:** slightly negative to neutral — retained metadata
  buffers per batch vs. no second text pass.
- **Required correctness oracle:** same differential corpus as 11.1, focused
  on proving the metadata carries everything the validator checks (width per
  row, per-cell text-type membership) or that the differences are exactly
  characterized and accepted; digest + error-witness parity.

### 11.3 Batch/column validation instead of row re-walk

- **Semantic risk: high.** The validator is row-oriented by contract: it
  attributes errors to rows and rejects ragged rows before decode acceptance
  semantics apply. Column-bulk checks over decoded columns cannot reproduce
  text-level membership (e.g. which textual spellings parse as Date32) and
  lose per-row failure attribution unless row indices are recomputed.
- **Error-timing risk: medium-high.** First-offending-row ordering within a
  batch would be lost unless explicitly recomputed; batch boundary timing is
  retained.
- **Expected memory effect:** positive on CPU, ambiguous on memory (masks /
  validity re-checks may allocate).
- **Required correctness oracle:** per-row accepted/rejected equivalence with
  row-number-preserving errors; fuzz corpus with multi-defect batches
  (which defect wins today vs. proposed).

### 11.4 Eliminate only provably redundant validation substeps

- **Semantic risk: low-medium (best of the four).** Keep the lockstep
  structure; remove only substeps provably duplicated by decode **with an
  explicit schema and `ignore_errors(false)`** — e.g. re-verifying that a cell
  Polars already typed as Int64 parses as Int64. The provability boundary is
  exactly the accept-set disagreement surface: dates/timestamps
  (`try_parse_dates` vs `chrono` patterns incl. RFC 3339 vs naive forms),
  float finiteness rules, bool spellings, empty-string nullability. Anything
  not proven must stay.
- **Error-timing risk: low.** Traversal order and batch boundaries unchanged;
  only the failing stage/category can flip for eliminated substeps (§8 shows
  decode already raises first for both measured drift classes).
- **Expected memory effect:** small positive (fewer parse attempts in the
  validator pass); bounded by the validation share (≤ 36 % of ingestion wall,
  of which the type-check substeps are ~0–41 % depending on dtype mix).
- **Required correctness oracle:** per-LogicalType accept-set equivalence
  proof (differential fuzz across every `LogicalType` variant: all int widths,
  floats incl. non-finite rejection, bool, Date32/Timestamp with and without
  timezone, empty/null handling) between `csv_value_matches` and the Polars
  cell parse under the exact production options; only types with a proven
  accept-set match may skip re-verification.

### 11.5 Ranking for a separate implementation task

1. **11.4 (redundant substeps) — recommended first step.** Lowest risk,
   contract-preserving structure, measurable target (type-check share of the
   26.5–36.0 % validation share), well-defined oracle.
2. **11.2 (metadata reuse)** — second, gated on the same oracle; larger gain
   (removes the text re-parse) with medium risk.
3. **11.1 (full fusion)** — largest gain (whole second pass) but high semantic
   and error-timing risk; only after 11.2's oracle exists and the disagreement
   surface is fully characterized.
4. **11.3 (column validation)** — not recommended as a standalone direction;
   row attribution and text-level membership make it the riskiest per unit of
   gain.

## 12. Recommendation

**GO** — for a separate, scoped implementation task (this task implements
nothing), justified by exact-head counted evidence:

- On full reads, the CSV/TSV path performs a second, full-range parse of the
  same bytes in lockstep: validator logical bytes = 100.00 % of file bytes and
  rows validated = rows decoded (ratio 1.000), counted on every fixture class
  (narrow, wide 128-col, long-UTF-8, anchor, 1 M rows, CSV and TSV).
- That second pass costs 26.5–36.0 % of ingestion wall (P50) on full reads
  (CPU-bound, not overlapped), so the achievable ceiling for a fusion task is
  ~26–36 % of connector ingestion wall — above the ≥30 % significance
  threshold PR #293 defined for ≥1 s ingestion workloads (the 1 M-row anchor
  sits at 2.4 s with tight noise, P95/P50 = 1.17).
- The duplicate work is confined to the consumed range under bounded reads
  (10.0–12.5 % shares match the consumed prefix exactly), so a fusion task
  must preserve bounded-read semantics (see parity laws) — the evidence shows
  bounded behavior is already prefix-exact and worth protecting.
- Condition: the implementation task must first build the §11 correctness
  oracles; start with candidate 11.4, proceed to 11.2, and treat 11.1 as a
  stretch goal. Failure-phase evidence (§8: decode already raises first for
  both measured drift classes) reduces — but does not eliminate — the
  error-timing risk, because the validator remains the authority for the
  classes not exercised here.

## 13. Acceptance criteria mapping (Issue #285)

- [x] Instrumentation is measurement-only and disabled by default — private
      `io-metrics` feature, extended under `#[cfg]`; un-instrumented path
      statement-identical; default builds unaffected (targeted tests green
      without the feature).
- [x] Duplicate work is counted, not inferred solely from code inspection —
      exact logical byte/row counters (§6.1, §7), fail-stage counters (§8).
- [x] Full and bounded/limited read cases are distinguished — §7 (preview
      row-limit and early-drop prefix cases; duplicate work follows the
      consumed prefix).
- [x] P50/P95, peak RSS and measurement noise are reported — §9 (5–7 reps per
      case, P50/P95/min/max, VmHWM, case-level allocations, within- and
      cross-campaign spread).
- [x] Malformed/schema-drift semantics are compared and documented — §8
      (failing stage counted, category/witness identical on/off) and §10
      (parity witnesses).
- [x] No production parser, validation, schema, API, dependency or
      resource-limit change — diff = `src/read.rs` cfg-gated counters/timers,
      one dev-dependency reference (`sha2`, already in the workspace lock; no
      new packages, no version changes), one new ignored test file, evidence
      docs. No `[features]` default change.
- [x] Final recommendation cites exact-head evidence — §12, head
      `f61e0853b67ff5ca7bedb0bddb707befb922baff`, records in
      `o0-c1-records.jsonl`.

Forbidden-surface confirmation: no CSV parser replacement, no production
fusion, no JSON/Parquet work, no dependency upgrade, no validation relaxation.

## 14. Deviations and disclosed limitations

- `/usr/bin/time` unavailable → peak RSS via `/proc` VmHWM (process-lifetime).
- `ingest_decode_nanos` is consumer-side blocking time; Polars decode-ahead
  means it is not exclusive decode CPU (validation time is exclusive).
- Phase-time P50s come from per-rep counter deltas (7/5 reps); counter values
  themselves were rep-stable (P50 = P95 for byte/row counters).
- Two campaigns were run (an early smoke campaign and the final one); the
  final campaign's records are the published ones. Absolute walls differ
  between campaigns under sibling load; ratio conclusions do not (§9).
- TSV support is measured through the shared delimited path (separator-only
  difference); JSON/NDJSON/Parquet are out of scope for this issue.
- The malformed fixtures place the defect at row 40 000, beyond the 100-row
  inference sample, so inference stays clean and the streaming stages fail —
  failure behavior for defects inside the inference prefix is out of scope.

## 15. Reproduction

See §5 for the exact commands. Fixtures regenerate deterministically
(`O0_C1_MODE=generate`); every record carries its fixture SHA-256, head SHA,
rep count, warm-up policy, and counter semantics. Parity runs need only the
feature-off binary (`cargo test --release -p
stillflow-connector-local-tabular --test o0_c1_csv_dup_work --no-run`).
