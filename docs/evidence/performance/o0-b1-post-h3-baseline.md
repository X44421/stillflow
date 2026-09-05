# O0-B1 — Post-H3 Performance Baseline on Current Main

- Issue: #282 (`[O0-B1] Refresh post-H3 performance baseline on current main`)
- Nature: **measurement only**. No production semantics, default feature flags,
  persistence format, API contract, or resource limit changed. No optimization
  is claimed and none is authorized by this document.
- Dispatch base: `main@f61e0853b67ff5ca7bedb0bddb707befb922baff`
- Exact measured head: `f61e0853b67ff5ca7bedb0bddb707befb922baff`
  (dispatched base == measured head; `origin/main` at dispatch time was the
  same commit, so all numbers below are from one exact head)
- Measurement-only harness commit (branch `agent/issue-282-o0-b1-baseline`):
  `a1add90` adds `tests/o0_b1_baseline.rs` plus dev-dependencies only — no
  production file is touched, so production content at every run equals the
  measured head above; the runs executed with this harness present (tree at or
  after `a1add90`), which the dispatch base alone does not describe.
- Branch: `agent/issue-282-o0-b1-baseline`
- Measurement date: 2026-09-05
- Raw per-case records (machine-readable, one JSON record per case):
  [`o0-b1-records.jsonl`](./o0-b1-records.jsonl)

## 1. Environment

| Item | Value |
| --- | --- |
| Host OS | WSL2 (Windows), kernel `6.18.33.2-microsoft-standard-WSL2 x86_64` |
| CPU | 12th Gen Intel(R) Core(TM) i3-12100F — 3 cores / 6 hardware threads (`nproc` = 6) |
| RAM | 12,249,204 kB total (~11.7 GiB) |
| Rust toolchain | `rustc 1.85.0 (4d91de4e4 2025-02-17)` — pinned by `rust-toolchain.toml` |
| Cargo | `cargo 1.85.0 (d73d2caf9 2024-12-31)` |
| Build profile | `cargo test --release`; the repo defines no `[profile.release]` override, so the test profile inherits release defaults (opt-level 3, codegen-units 16, no LTO). The `[profile.test]` `debug = 0` override applies only to the dev-profile test build, not these measurements. |
| Shared target dir | `CARGO_TARGET_DIR=/home/owl/.cargo-o0-target` (sibling conversations share build artifacts; cargo's file lock serializes builds) |
| Concurrency policy | Other agent conversations ran in parallel on sibling worktrees. **Every timed run was executed inside `flock /tmp/stillflow-o0-measure.lock`**, so only one task measured at a time. Untimed work (fixture generation, coding, building) ran outside the lock. |
| RSS/CPU tooling | `/usr/bin/time` is not installed in this environment (disclosed limitation). Peak RSS comes from in-process `/proc/self/status` `VmHWM` (process lifetime); CPU time from `/proc/self/stat` `utime+stime` deltas (10 ms tick granularity). This is the `/proc` sampling path the issue explicitly allows. |

## 2. Method

### 2.1 Harness

New measurement-only integration test
`backend/crates/stillflow-connector-local-tabular/tests/o0_b1_baseline.rs`:

- feature-gated behind the private `io-metrics` feature and `#[ignore]`d, so
  default CI and default builds never run or compile it into behavior;
- one case per process (`O0_B1_CASE`), so peak RSS and CPU cover exactly one
  case;
- **reuses** the accepted E24-B2BASE infrastructure instead of rewriting it:
  - the `io-metrics` logical-byte/parser-invocation counters and their dump
    side channel (`E24_IO_METRICS_OUT` — the historical variable name is kept),
    introduced for the #99 baseline, measure this run unchanged;
  - the `anchor-*` fixture generators are copied verbatim from
    `tests/read_baseline.rs` so fixture identity is preserved across baselines;
- the engine-E2E cases drive the real `ExecutionEngine::materialize` path
  (preflight → connector read → rules → `SnapshotWriter` append/commit) with
  the real `LocalTabularConnector` and a real `SnapshotStore` per run;
- no production file was touched. The only Cargo change is dev-dependency
  additions (`stillflow-engine`, `stillflow-plan`, `stillflow-storage`, `sha2`)
  to `stillflow-connector-local-tabular`, which affect test builds of that
  crate only.

### 2.2 Warm-up, run counts, aggregation

- Warm-up: exactly 1 untimed run per case (Polars thread-pool init, page-cache
  warm for the freshly generated fixture) before the timed loop.
- Run counts: 30 reps for the five cross-baseline anchor cases (matching the
  #99 baseline's rep count), 7 or 5 reps for all other cases (issue minimum is
  5). Every case reports its `reps`.
- Wall time: `std::time::Instant` around the full read/materialize; **P50 and
  P95** use nearest-rank percentiles; min/max are also recorded.
- CPU time: `/proc/self/stat` utime+stime delta per run (×10 ms tick); the
  P50 is reported. All cases had reliable CPU readings.
- Peak RSS: process-lifetime `VmHWM` at case end (includes warm-up; not
  per-run — attribution disclosed in every record).
- Noise bound: inter-run spread reported as `P95 / P50` per case (§7).

### 2.3 Logical I/O vs physical I/O

The `io-metrics` counters used here are **logical** counters:

- `validator_read_bytes` — exact logical bytes pulled through the CSV
  validator-pass `CountingReader` (read.rs `io_metrics::CountingReader`);
- `json_handle_bytes` — exact logical bytes through the JSON framing-pass
  `CountingReader`;
- `json_framed_bytes`, `json_reencode_bytes` — exact logical bytes framed and
  re-serialized before the Polars JSON decode;
- `inference_phase_bytes` — exact logical bytes consumed by the bounded schema
  inference pass;
- `csv_decoder_invocations`, `csv_rows_validated`,
  `json_polars_decode_invocations`, `parquet_reader_constructions`,
  `parquet_batch_finishes` — parser invocation counters;
- `decoder_os_bytes` is recorded at handle-open time and is a
  **handle/OS-level observation, not exact logical read bytes** (Polars may
  mmap); it is labeled as such in every record and is never used as a logical
  read count.

No page-cache-sensitive physical device counters (e.g. `/proc/<pid>/io
read_bytes`) are used anywhere in this baseline.

### 2.4 Correctness witness

Every case records:

- per-run row-count stability across all reps (`rows_stable_across_reps`);
- an untimed witness run whose emitted `BatchEnvelope` payloads are digested
  with SHA-256 over each Arrow `ArrayData` (dtype, length, buffer bytes, null
  count), plus the logical schema (name:type:nullable per field);
- engine cases additionally read the published snapshot back through
  `SnapshotStore::read_batches` and digest those batches, and record the
  manifest row count, stored byte count, and partition count;
- cross-process reproducibility was observed: identical cases measured in
  independent processes produced identical digests (e.g.
  `engine-narrow-simple-8c-100k` → `165ce753…`, `engine-rule-heavy-8c-100k` →
  `ed4a7ff9…`), and the pass-through engine digest equals the raw ingestion
  digest for the same fixture (`165ce753…` for
  `ingest-csv-narrow-fixed-8c-100k` and `engine-narrow-simple-8c-100k`);
- malformed cases record the stable error category, retryability, and message
  (7/7 reps identical).

## 3. Fixtures

All fixtures are generated deterministically by the harness (byte-identical
across runs; SHA-256 recorded in every record). The `anchor-*` generators are
the E24-B2BASE generators from `tests/read_baseline.rs` **verbatim** (fixture
bytes match the historical baseline exactly, e.g. csv/10c/100k = 65,000,115
bytes in both baselines). Other generators are new, deterministic O0-B1
fixtures; none replaces a historical fixture.

| Fixture (case family) | File | Rows | Cols | Bytes | SHA-256 (first 16) | Generator identity |
| --- | --- | ---: | ---: | ---: | --- | --- |
| anchor-csv-10c-100k | f.csv | 100,000 | 10 | 65,000,115 | `1a93d7f2f3ec3335` | E24-B2BASE verbatim (uniform 32–96 B variable UTF-8 cells) |
| anchor-csv-100c-100k | f.csv | 100,000 | 100 | 650,000,360 | `ff1d8f254f536d26` | E24-B2BASE verbatim |
| anchor-csv-10c-1m | f.csv | 1,000,000 | 10 | 650,000,090 | `07b046503f26ff91` | E24-B2BASE verbatim |
| anchor-ndjson-10c-100k | f.ndjson | 100,000 | 10 | 72,200,085 | `03c934db69f22537` | E24-B2BASE verbatim |
| anchor-ndjson-100c-100k | f.ndjson | 100,000 | 100 | 729,199,970 | `94bddd5c3faea998` | E24-B2BASE verbatim |
| anchor-array-10c-100k | f.json | 100,000 | 10 | 72,200,087 | `2e7c73e408519368` | E24-B2BASE verbatim (top-level JSON array) |
| anchor-parquet-10c-100k | f.parquet | 100,000 | 10 | 13,509,276 | `b8720a3f67cfbe42` | E24-B2BASE verbatim (SNAPPY, 8,192-row chunks) |
| anchor-parquet-100c-100k | f.parquet | 100,000 | 100 | 134,904,092 | `12fb0e496e1c36f9` | E24-B2BASE verbatim |
| narrow-fixed-8c (required: narrow fixed-width) | f.csv | 100,000 / 1,000,000 | 8 | 10,200,024 / 102,000,024 | `367b1e2910afc8c5` / `3ea0f8e32359ac05` | O0-B1 (fixed-width ints/floats + fixed 12-char strings) |
| wide-mixed-128c (required: wide mixed schema) | f.csv | 100,000 | 128 | 108,393,184 | `173dc23df39ba831` | O0-B1 (Int64/Float64/Utf8/Int64 column cycle) |
| longutf8-8c (required: long UTF-8 / variable width) | f.csv | 100,000 | 8 | 109,990,716 | `b4733c8c8f0e9e01` | O0-B1 (per-row 32–2,048 B payload) |
| timestamps-10c (required: timestamp/timezone JSON) | f.ndjson | 100,000 | 6 | 15,658,588 | `726639739e80216a` | O0-B1 (ISO-8601 UTC `Z`, `+09:00` offsets, date, tz name) |
| malformed-csv-10c-60k (required: error case) | f.csv | 60,000 | 10 | 6,119,942 | `eeddffcfe69a6210` | O0-B1 (short 3-field row at row 40,000) |
| malformed-ndjson-10c-30k (required: error case) | f.ndjson | 30,000 | 10 | 4,578,760 | `a2349fc11322da98` | O0-B1 (broken JSON line at row 15,000) |

## 4. Results — ingestion microbenchmarks

Connector `read_batches` drain only (no engine, no storage). Wall time in ms;
CPU P50 in ms; RSS in MiB (process lifetime).

| Case | Fixture | P50 | P95 | min–max | CPU P50 | Peak RSS | Reps | Rows out |
| --- | --- | ---: | ---: | --- | ---: | ---: | ---: | ---: |
| ingest-csv-anchor-10c-100k | 10c×100k CSV | 381 | 471 | 324–541 | 390 | 171 | 30 | 100,000 |
| ingest-csv-anchor-100c-100k | 100c×100k CSV | 3,177 | 3,550 | 3,006–3,638 | 3,190 | 1,473 | 30 | 100,000 |
| ingest-csv-anchor-10c-1m | 10c×1M CSV | 3,134 | 3,304 | 2,749–3,304 | 3,150 | 1,396 | 7 | 1,000,000 |
| ingest-ndjson-anchor-10c-100k | 10c×100k NDJSON | 1,261 | 1,588 | 1,028–1,591 | 1,470 | 148 | 30 | 100,000 |
| ingest-ndjson-anchor-100c-100k | 100c×100k NDJSON | 20,724 | 26,603 | 19,815–26,603 | 22,780 | 1,138 | 7 | 100,000 |
| ingest-json-array-anchor-10c-100k | 10c×100k JSON array | 2,057 | 3,101 | 1,661–3,615 | 2,230 | 143 | 30 | 100,000 |
| ingest-parquet-anchor-10c-100k | 10c×100k Parquet | 610 | 748 | 489–842 | 620 | 119 | 30 | 100,000 |
| ingest-parquet-anchor-100c-100k | 100c×100k Parquet | 5,156 | 6,303 | 4,718–6,303 | 5,170 | 997 | 7 | 100,000 |
| ingest-csv-narrow-fixed-8c-100k | narrow fixed 8c | 94 | 117 | 92–117 | 110 | 43 | 7 | 100,000 |
| ingest-csv-wide-mixed-128c-100k | wide mixed 128c | 1,051 | 1,070 | 982–1,070 | 1,060 | 305 | 7 | 100,000 |
| ingest-csv-longutf8-8c-100k | long UTF-8 8c | 541 | 671 | 517–671 | 550 | 248 | 7 | 100,000 |
| ingest-ndjson-timestamps-10c-100k | timestamp NDJSON | 496 | 546 | 491–546 | 570 | 40 | 7 | 100,000 |

Digest witnesses (SHA-256 over emitted Arrow data) are in
`o0-b1-records.jsonl`; all row counts and digests were stable across reps and
across independent processes.

### Logical I/O and parser counters (last timed run per case)

| Case | Logical counters (exact) | OS-level (not logical) | Parser invocations |
| --- | --- | --- | --- |
| csv 10c×100k | validator_read_bytes = 65,000,115 (= file); inference_phase_bytes = 1,048,576 (bounded) | decoder_os_bytes = 65,000,115 | csv_decoder_invocations = 99; csv_rows_validated = 100,000 |
| csv 100c×100k | validator_read_bytes = 650,000,360 (= file); inference 1,048,576 | decoder_os_bytes = 650,000,360 | csv_decoder_invocations = 99; csv_rows_validated = 100,000 |
| ndjson 10c×100k | json_handle_bytes = 72,200,085 (= file); json_framed_bytes = 72,172,250; json_reencode_bytes = 72,200,085; inference 1,048,576 | — | json_polars_decode_invocations = 25 |
| json array 10c×100k | json_handle_bytes = 72,200,087; json_framed_bytes = 72,172,250; json_reencode_bytes = 72,200,085 | — | json_polars_decode_invocations = 25 |
| parquet 10c×100k | — | decoder_os_bytes = 13,509,276 | parquet_reader_constructions = 27; parquet_batch_finishes = 26 |

Observations on the exact measured head (all from logical counters only):

- **CSV**: the validator pass logically re-reads 100% of the file
  (`validator_read_bytes` == file size on every complete read), in lockstep
  with the decoder, plus a bounded third inference pass. This confirms on
  current main that the O0-D0 §3.2 C1 "double parse + bounded third pass"
  structure is still present.
- **JSON**: one full framing pass (`json_framed_bytes` ≈ file size) plus one
  full re-serialization pass (`json_reencode_bytes` == file size) before the
  Polars JSON decode. O0-D0 C2's multi-pass chain is still present; NDJSON is
  the slowest per-byte format in the matrix (100c×100k = 20.7 s).
- **Parquet**: 27 reader constructions for 26 batch finishes — the per-chunk
  reader rebuild noted in O0-D0 C3 is still present.

## 5. Results — engine E2E (preflight + read + rules + snapshot write)

`ExecutionEngine::materialize` with the real connector and a fresh
`SnapshotStore` per rep (store open and temp-dir creation are outside the
timed region; begin/append/commit are inside, matching the production call
path). `stored_byte_count` is the manifest's logical written-bytes figure for
the published snapshot (content-addressed Parquet partitions, SNAPPY).

| Case | Plan shape | P50 | P95 | min–max | CPU P50 | Peak RSS | Reps | Rows out | Partitions | Stored bytes |
| --- | --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| engine-narrow-simple-8c-100k | scan → materialize | 375 | 488 | 342–488 | 370 | 37 | 7 | 100,000 | 25 | 5,165,208 |
| engine-wide-mixed-128c-100k | scan → materialize (128 cols) | 2,862 | 3,483 | 2,803–3,483 | 2,870 | 187 | 5 | 100,000 | 25 | 76,062,958 |
| engine-rule-heavy-8c-100k | scan → 48 rules → materialize | 601 | 652 | 557–652 | 650 | 56 | 7 | 100,000 | 25 | 12,970,192 |
| engine-expression-heavy-8c-100k | scan → filter → 16 boolean derives (25-node exprs) + FilterRows → materialize | 841 | 969 | 804–969 | 980 | 47 | 7 | 100,000 | 25 | 5,326,591 |
| engine-parquet-100c-100k | parquet scan → materialize | 10,259 | 10,672 | 9,584–10,672 | 10,260 | 769 | 5 | 100,000 | 25 | 148,629,736 |
| engine-ndjson-timestamps-override-10c-100k | scan → materialize with schema override (Timestamp{ms,UTC} + Date32) | 1,036 | 1,299 | 814–1,299 | 1,160 | 35 | 7 | 100,000 | 25 | 2,775,772 |
| engine-narrow-write-8c-1m | scan → materialize (1M rows, 245 partitions) | 2,575 | 2,646 | 2,548–2,646 | 2,580 | 137 | 5 | 1,000,000 | 245 | 51,645,808 |

Notes:

- Rule/expression plan deltas versus the pass-through plan on the same fixture
  (attribution by difference, not a phase-isolated measurement): 48 mixed
  rules add ~+226 ms P50 (+60%) over `engine-narrow-simple`; 16 boolean
  derives with ~25-node expressions plus a filter node add ~+466 ms (+124%).
  All arithmetic operators (Add/Subtract/Multiply/Divide/Modulo) are **paused
  on this head** ("checked arithmetic is paused until overflow semantics are
  implemented"), so the expression-heavy workload uses the supported
  comparison/boolean surface; the arithmetic-expression cost is therefore
  unmeasured here.
- The timestamp/timezone override path **succeeds on this head**: the output
  schema witness contains `event_utc: Timestamp { unit: Millisecond, timezone:
  Some("UTC") }` and `event_date: Date32`, with 100,000/100,000 rows and a
  digest-stable witness. This is an H3-era behavioral observation; it is not
  evidence about default enablement of any JSON feature (see §9).
- Write-path sizing: `engine-narrow-write-8c-1m` publishes 1M rows as 245
  partitions / 51.6 MB stored bytes in ~2.6 s including the full CSV
  dual-parse read of a 102 MB fixture.

## 6. Results — failure timing (malformed inputs)

| Case | P50 | P95 | Reps | Failure point | Error witness (stable 7/7) |
| --- | ---: | ---: | ---: | --- | --- |
| ingest-csv-malformed-10c-60k | 2 | 3 | 7 | First Polars decode block containing the bad row (row 40,000 of 60,000); the lockstep validator never validates a row (`csv_rows_validated = 0`) | `InvalidData`, non-retryable, "source data is malformed or incompatible with the established schema" |
| ingest-ndjson-malformed-10c-30k | 138 | 157 | 7 | Mid-stream, at row 15,001, after 12,288 rows had been framed | `InvalidData`, non-retryable, "JSON row does not match the established schema at row 15001" |

Failure timing is part of the baseline: a malformed CSV fails in milliseconds
(decoder block granularity, warm page cache), while a malformed NDJSON fails
mid-stream after processing roughly half the file. Error categories are
`InvalidData`/non-retryable for both.

## 7. Noise and minimum meaningful improvement threshold

Inter-run spread per case (P95/P50 of wall time, all reps under the shared
measurement lock):

| Workload class | Observed P95/P50 |
| --- | --- |
| Engine E2E ≥ 500 ms (narrow/simple, rule-heavy, wide, expression, parquet, 1M write) | 1.03 – 1.30 |
| Ingestion ≥ 1 s (csv 100c, ndjson 100c, parquet 100c, csv 1m) | 1.02 – 1.28 |
| Fast ingestion < 1 s (10c anchors, narrow-fixed, timestamps, longutf8) | 1.10 – 1.51 (scheduler noise dominates; min–max spread up to 2.2× on 30-rep runs) |

Minimum meaningful improvement threshold for follow-up work, derived from the
observed noise (a claim must clear the noise, not a single run):

- **Engine E2E workloads**: a P50 reduction of **≥ 25%** over ≥ 5 runs, with
  the full inter-run range of the improved build below the baseline P95, is
  the minimum detectable, meaningful improvement.
- **Ingestion ≥ 1 s**: **≥ 30%** P50 reduction under the same discipline.
- **Sub-second ingestion micro cases**: **≥ 50%** P50 reduction, or an
  increase to ≥ 30 reps before claiming.
- No optimization claim may rest on a single run (existing hard rule); the
  spreads above are the quantified reason.

## 8. Cross-baseline historical context (NOT apples-to-apples)

The last trustworthy historical ingestion measurements are the accepted
E24-B2BASE numbers recorded on Issue #99 (PR #100 evidence, 30 reps): base
`main@636cd7db443bed45e7adcf1596785670cfc3ff1c`, harness head
`3493f22409b7ceae6bd55c71579d783fa931d2e6`, **host environment undocumented**
(measurement artifacts were captured on an external drive), and a much older
main. The fixture generator — and therefore the fixture bytes — is identical;
the machine, OS scheduling environment, and head are **not**.

**This table is historical context only. It is not an apples-to-apples
regression test and must not be read as a regression or improvement claim.**

| Format | Cols | Rows | E24-B2BASE P50/P95 (ms) | O0-B1 P50/P95 (ms) | Ratio (O0-B1/E24 P50) |
| --- | ---: | ---: | --- | --- | ---: |
| csv | 10 | 100,000 | 264 / 303 | 381 / 471 | 1.44 |
| csv | 100 | 100,000 | 2,848 / 3,268 | 3,177 / 3,550 | 1.12 |
| ndjson | 10 | 100,000 | 762 / 796 | 1,261 / 1,588 | 1.65 |
| json array | 10 | 100,000 | 1,373 / 1,491 | 2,057 / 3,101 | 1.50 |
| parquet | 10 | 100,000 | 294 / 423 | 610 / 748 | 2.07 |

Ratios between 1.1× and 2.1× on identical fixture bytes most plausibly reflect
the undocumented historical host and environment differences plus post-#99
main drift (H1–H3 landed substantially more runtime, storage, verification,
and temporal machinery). No per-commit attribution is possible from two
points, and none is claimed. What this table does establish: the O0-D0-era
per-format *structure* (C1/C2/C3 above) is unchanged on current main, while
absolute numbers must come from this document's §4/§5 only.

## 9. Decision output — GO / NO-GO / INCONCLUSIVE for candidate areas

Decision rules: GO = this baseline's measurements show the targeted overhead
exists on the exact head AND its cost share is material relative to the §7
noise threshold; NO-GO = not supported (or policy-forbidden) on this evidence;
INCONCLUSIVE = the overhead exists but this baseline does not isolate its cost
share. **Nothing here authorizes implementation.**

| Candidate area | Measured evidence on this head | Decision |
| --- | --- | --- |
| Connector single-pass decode (CSV second full parser; JSON framing/re-encode passes) — O0-D0 H2/C1+C2 | Logical counters show CSV validator re-reads 100% of file bytes (lockstep) + bounded inference; JSON re-encodes 100% of file bytes before the Polars parse. NDJSON 100c/100k = 20.7 s and csv 100c/100k = 3.18 s are the dominant ingestion costs; well above the §7 thresholds. | **GO** (candidate for a separately gated optimization task) |
| Parquet per-chunk reader rebuild — O0-D0 C3 | 27 reader constructions / 26 batch finishes confirm the rebuild exists; its cost share is not isolated (parquet ingest 100c/100k = 5.16 s includes decode itself). | **INCONCLUSIVE** |
| Engine predictor / schema-clone cost — O0-D0 H1 | Rule/expression deltas (+226 ms / +466 ms P50) bound the whole rules path, but predict cost specifically is not isolated here. | **INCONCLUSIVE** (dedicated predictor-cost measurement task O0-P1 is separately dispatched) |
| Snapshot write-path single-I/O — O0-D0 S2/H3 | Write path measured only inside E2E (§5); the write-path digest re-read and read-side double logical read are not separately isolated here. | **INCONCLUSIVE** |
| JSON direct-projected writer default enablement | Out of scope for this baseline: a measurement-only task, and nothing here authorizes enablement. The §5 timestamp-override success is a behavioral observation, not enablement evidence. Since dispatch, issue #151 has closed (connector-side fix in PR #225) and the O0-J1 revalidation (issue #283, PR #294) judges the old #151 blocker obsolete for this path; default enablement remains a separate decision, not a product of this note. | **NO-GO** |
| Lowering-cache revival (based on old #152 results) | Policy-forbidden; #152-era results are not admissible against this head. | **NO-GO** |
| Predictor optimization | Policy-forbidden by this task; measurement-only follow-up (O0-P1) authorized separately. | **NO-GO** |
| Persistence-format change | Contract surface; not measured here. | **NO-GO** |
| Executor architecture work | Not measured here. | **NO-GO** |

## 10. Acceptance criteria mapping

- [x] All reported numbers are produced on one exact measured head
      (`f61e0853…`, equal to the dispatched base) and one documented
      environment (§1).
- [x] P50/P95 and peak RSS exist for every primary workload (§4, §5, §6;
      RSS attribution disclosed per record).
- [x] Logical I/O / parser counters are used where available
      (`io-metrics` logical counters) and are explicitly distinguished from
      handle/OS-level observations; no physical device I/O counters are used
      (§2.3).
- [x] Correctness witnesses match expected outputs: stable row counts, stable
      cross-rep and cross-process SHA-256 digests, schema witnesses (including
      renamed/derived/timestamp fields), snapshot read-back with manifest row
      counts, stored byte counts, and partition counts (§2.4, §5).
- [x] No production semantics, default feature flags, persistence format, API
      contract, or resource limit changes: diff = one new measurement-only
      test file + dev-dependencies in one crate's `[dev-dependencies]`
      (+Cargo.lock dev-dep references; no new packages, no version changes).
- [x] No optimization claim is made from a single run; all claims are
      distributional (§7 threshold discipline).
- [x] Noise is quantified (P95/P50 per workload, §7) and a minimum meaningful
      improvement threshold is defined for follow-up work.
- [x] `cargo fmt --all -- --check` passes; targeted tests for the touched
      crate pass (`cargo test -p stillflow-connector-local-tabular`:
      23 unit + 26 + 11 + 1 integration tests, 8 evidence tests ignored as
      designed; `--features io-metrics` test build compiles clean).

## 11. Reproduction

```bash
export CARGO_TARGET_DIR=/home/owl/.cargo-o0-target
cd backend
cargo test --release -p stillflow-connector-local-tabular \
  --features io-metrics --test o0_b1_baseline --no-run
BIN=$(ls -t "$CARGO_TARGET_DIR"/release/deps/o0_b1_baseline-* | grep -v '\.d$' | head -1)
# fixture generation (untimed, outside the lock):
O0_B1_CASE=ingest-csv-anchor-10c-100k O0_B1_MODE=generate "$BIN" \
  --exact --ignored --nocapture o0_b1_baseline
# timed run (must be serialized with the shared measurement lock):
flock /tmp/stillflow-o0-measure.lock -c \
  'O0_B1_CASE=ingest-csv-anchor-10c-100k O0_B1_MODE=measure "$BIN" \
   --exact --ignored --nocapture o0_b1_baseline'
```

Known environment limitations (disclosed, not passed as passes): `/usr/bin/time`
is unavailable (peak RSS via `/proc` VmHWM instead); CPU tick quantization is
10 ms; peak RSS is process-lifetime (includes warm-up) rather than per-run;
WSL2 scheduler noise on sub-second workloads is large enough that those cases
require the §7 elevated thresholds.

## 12. Scope of changes (completion report)

- Modified files:
  - `backend/crates/stillflow-connector-local-tabular/Cargo.toml`
    (dev-dependencies only)
  - `backend/Cargo.lock` (dev-dependency references only; no new packages, no
    version changes)
  - `backend/crates/stillflow-connector-local-tabular/tests/o0_b1_baseline.rs`
    (new, measurement-only)
  - `docs/evidence/performance/o0-b1-post-h3-baseline.md` (this document)
  - `docs/evidence/performance/o0-b1-records.jsonl` (raw records)
- New dependencies: none outside the existing workspace lock (dev-dependency
  references to already-locked workspace crates and `sha2`).
- Public API changes: none.
- unwrap/expect usage: confined to the ignored, feature-gated measurement
  test (fixture generation and witness paths), consistent with the existing
  `tests/read_baseline.rs` style.
- TODO items: none.
- Test results: §10.
- Contract deviations: none.
- Remaining risks: single-host, single-run-campaign numbers; WSL2 timing noise
  on sub-second cases; engine arithmetic-expressions unmeasured (paused
  upstream surface).
- Branch: `agent/issue-282-o0-b1-baseline`
