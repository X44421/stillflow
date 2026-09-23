# P55-B1 — Polars 0.55.2 performance baseline

- Issue: #399 (`[P55] Polars 0.46 → 0.55.2 分阶段迁移`)
- Nature: **measurement only**. No production semantics, default feature flag,
  persistence format, API contract or resource limit is changed, and no
  optimization is claimed or authorized by this document. The legacy O0/O1
  numbers are quoted as a *reference point only*: the compiler, the dependency
  graph and the physical executor all changed at once, so this document never
  reads a difference against them as an upgrade gain.
- Measured head (polars `0.55.2`): `aa900f5` (production content: P55-D1/A1/A2 + P55-E1)
- Measurement-only harness commit (this branch, production files untouched):
  `fdb600bb1a0a1295f357fd87bb19bddffe484e85` — adds the `ingest-csv-anchor-100c-1m` case and its fixture arm
- Reference head (polars `0.46`, rustc `1.85.0`): `f61e0853b67ff5ca7bedb0bddb707befb922baff`
  (O0-B1 baseline, [`o0-b1-post-h3-baseline.md`](../performance/o0-b1-post-h3-baseline.md))
- Branch: `agent/issue-399-p55-b1-performance-baseline`
- Measurement date: 2026-09-22
- Raw per-case records (machine-readable, one JSON record per case):
  [`p55-b1-records.jsonl`](./p55-b1-records.jsonl)

## 1. Environment

| Item | Value |
| --- | --- |
| Host OS | Debian GNU/Linux 13 (trixie) on WSL2, kernel `6.18.33.2-microsoft-standard-WSL2 x86_64` |
| CPU / RAM | 3 cores / 6 hardware threads (`nproc` = 6) / ~11 GiB |
| Rust toolchain | `rustc 1.98.0 (88d9e12ae 2026-08-18)`, `cargo 1.98.0` (P55-T0 pin) |
| Build profile | `cargo test --release` (polars `0.55.2`) |
| Build concurrency | `jobs = 2` (unchanged repository convention) |
| Measurement discipline | one case per process (`O0_B1_CASE`), `flock`-serialized, no other cargo job on the host during the measured window |
| Peak RSS | process-lifetime `/proc` `VmHWM` including warm-up, not per-run (same method as the reference baseline) |
| Fixture identity | every record carries the fixture SHA-256 and byte size |

## 2. Method

The repository's existing O0-B1 measurement harness
(`backend/crates/stillflow-connector-local-tabular/tests/o0_b1_baseline.rs`) is
reused unchanged except for one added case:

- `ingest-csv-anchor-100c-1m` — the wide-and-deep cell the migration plan names
  explicitly (100 columns × 1,000,000 rows, 6.5 GB CSV, 3 reps). It has no
  counterpart in the reference baseline, so it is reported as a new cell rather
  than as a comparison.

Every case records wall time (p50/p95/min/max over its rep count), CPU time,
peak RSS, decoded-row and error stability, a semantic witness (canonical
digest of the decoded batches plus the reconstructed schema) and the
`io-metrics` counters (validator/decoder byte accounting, decoder invocations,
decode/validate wall nanos).

## 3. Semantic identity first

Before any timing is read, the new records are checked against the reference
records for **input and output identity**:

- fixture `sha256` — same generator, same bytes;
- witness `digest` — canonical digest of the decoded batches;
- witness `rows` and reconstructed schema.

All 21 shared cases have identical fixture SHA-256 values and matching row
counts. The 19 successful cases have identical witness digests and reconstructed
schemas. The two malformed-input cases have no output digest by construction;
they fail closed with zero decoded rows and matching stable error witnesses.
`rows_stable_across_reps` is true for every case.

The per-case digests are in §4. This is a byte-level output parity check for
all 19 successful shared cases, with matching error witnesses for the two
failure cases. It is stronger than the suite-level parity recorded in
[P55-E1](../p55/p55-e1-semantic-parity.md) and closes that document's first
residual risk.

## 4. Results

`=` marks a cell whose fixture hash and witness digest equal the reference
baseline record; `(new)` marks the cell that has no reference counterpart.

### engine-e2e

| case | fixture sha256 | witness digest | rows | reps | old p50 ms | new p50 ms | new p95 ms | old RSS MiB | new RSS MiB | batches (last run) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `engine-expression-heavy-8c-100k` | `367b1e2910af…` = | `6841d53d716c…` = | 100000 | 7 | 841 | 595 | 654 | 47 | 57 | 27 |
| `engine-narrow-simple-8c-100k` | `367b1e2910af…` = | `165ce7537bdb…` = | 100000 | 7 | 375 | 189 | 206 | 38 | 37 | 27 |
| `engine-narrow-write-8c-1m` | `3ea0f8e32359…` = | `eb31f30daa2e…` = | 1000000 | 5 | 2575 | 1723 | 1741 | 137 | 134 | 247 |
| `engine-ndjson-timestamps-override-10c-100k` | `726639739e80…` = | `292656382dbc…` = | 100000 | 7 | 1036 | 502 | 559 | 36 | 37 | 25 |
| `engine-parquet-100c-100k` | `12fb0e496e1c…` = | `4c75f7ea6de1…` = | 100000 | 5 | 10259 | 6854 | 7522 | 770 | 788 | 26 |
| `engine-rule-heavy-8c-100k` | `367b1e2910af…` = | `ed4a7ff9d18d…` = | 100000 | 7 | 601 | 411 | 432 | 56 | 58 | 27 |
| `engine-wide-mixed-128c-100k` | `173dc23df39b…` = | `95ba84aa2086…` = | 100000 | 5 | 2862 | 1958 | 2068 | 188 | 165 | 27 |

### ingest-micro

| case | fixture sha256 | witness digest | rows | reps | old p50 ms | new p50 ms | new p95 ms | old RSS MiB | new RSS MiB | batches (last run) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `ingest-csv-anchor-100c-100k` | `ff1d8f254f53…` = | `4c75f7ea6de1…` = | 100000 | 30 | 3177 | 2508 | 2595 | 1473 | 1407 | 27 |
| `ingest-csv-anchor-100c-1m` | `7f67f2231068…` (new) | `3149dda35e39…` (new) | 1000000 | 3 | - | 23869 | 23921 | - | 10689 | 247 |
| `ingest-csv-anchor-10c-100k` | `1a93d7f2f3ec…` = | `836cd9ab36a6…` = | 100000 | 30 | 381 | 232 | 339 | 171 | 156 | 27 |
| `ingest-csv-anchor-10c-1m` | `07b046503f26…` = | `5a8afcbba729…` = | 1000000 | 7 | 3134 | 2188 | 2227 | 1397 | 1369 | 247 |
| `ingest-csv-longutf8-8c-100k` | `b4733c8c8f0e…` = | `5c7a0fd222c1…` = | 100000 | 7 | 541 | 456 | 493 | 249 | 233 | 27 |
| `ingest-csv-malformed-10c-60k` | `eeddffcfe69a…` = | none (error case) | 0 | 7 | 2 | 0 | 1 | 15 | 16 | 1 |
| `ingest-csv-narrow-fixed-8c-100k` | `367b1e2910af…` = | `165ce7537bdb…` = | 100000 | 7 | 94 | 59 | 64 | 43 | 42 | 27 |
| `ingest-csv-wide-mixed-128c-100k` | `173dc23df39b…` = | `95ba84aa2086…` = | 100000 | 7 | 1051 | 757 | 860 | 305 | 272 | 27 |
| `ingest-json-array-anchor-10c-100k` | `2e7c73e40851…` = | `836cd9ab36a6…` = | 100000 | 30 | 2057 | 1224 | 1359 | 144 | 123 | 25 |
| `ingest-ndjson-anchor-100c-100k` | `94bddd5c3fae…` = | `4c75f7ea6de1…` = | 100000 | 7 | 20724 | 12699 | 18469 | 1138 | 849 | 25 |
| `ingest-ndjson-anchor-10c-100k` | `03c934db69f2…` = | `836cd9ab36a6…` = | 100000 | 30 | 1261 | 856 | 1027 | 148 | 113 | 25 |
| `ingest-ndjson-malformed-10c-30k` | `a2349fc11322…` = | none (error case) | 0 | 7 | 138 | 94 | 104 | 24 | 25 | 3 |
| `ingest-ndjson-timestamps-10c-100k` | `726639739e80…` = | `880a3b46b5c4…` = | 100000 | 7 | 496 | 380 | 413 | 40 | 36 | 25 |
| `ingest-parquet-anchor-100c-100k` | `12fb0e496e1c…` = | `4c75f7ea6de1…` = | 100000 | 7 | 5156 | 3733 | 3865 | 998 | 1018 | 26 |
| `ingest-parquet-anchor-10c-100k` | `b8720a3f67cf…` = | `836cd9ab36a6…` = | 100000 | 30 | 610 | 392 | 428 | 120 | 123 | 26 |

## 5. Noise and thresholds

Per-case p50/p95 spread is small for the ingest cases (p95 within ~20 % of p50) and
wider for the sub-second engine cases, matching the reference baseline's
observation that WSL2 scheduler noise dominates short workloads. The new
100-column × 1M-row CSV cell has a 6.5 GB fixture, 23.869 s wall p50 and
23.940 s process CPU p50. These measurements do not isolate disk time from
parsing time, so no bottleneck attribution is made.

Every new p50 in §4 is numerically below its reference value. **No improvement
is claimed and none is authorized:** the compiler moved 1.85.0 → 1.98.0, Polars
moved 0.46 → 0.55.2 with a new expression/stream/OOC layer, and the A1/A2
adapters changed where decoding and lowering run. These numbers are the new
baseline for the engine-modernization line, not a verdict on the upgrade.

One observation that is worth carrying into follow-up work: peak RSS tracks the
*file size*, not the batch contract. The connector decodes one bounded window at
a time, but the CSV path memory-maps the source, so resident pages of the mapped
file count against `VmHWM` while they stay resident:

| cell | fixture | peak RSS | RSS / fixture |
| --- | --- | --- | --- |
| `ingest-csv-anchor-100c-1m` | 6.50 GB | 10,945,320 KiB (10.4 GiB) | 1.72× |
| `ingest-csv-anchor-100c-100k` | 650 MB | 1,441,136 KiB (1.37 GiB) | 2.27× |
| `ingest-csv-anchor-10c-1m` | 650 MB | 1,401,432 KiB (1.34 GiB) | 2.21× |

The reference baseline recorded the same order of magnitude for its cells
(1473 MiB for the 650 MB `100c-100k` fixture, 2.27×), so this is unchanged
behaviour rather than a 0.55 regression. It does mean the widest and deepest
cell needs a host that can hold the mapping — on a smaller machine it would
thrash. Bounding mapped residency is a candidate for the engine-modernization
line, not a defect introduced by the upgrade.

## 6. Residual risk / limitations

- Release-profile numbers on this WSL2 host are noisy for sub-second cases; the
  reference baseline already documented that and used elevated thresholds for
  them. The per-case rep counts are unchanged, so p50/p95 here carry the same
  caveat.
- `jobs = 2` is a build convention; measurement throughput itself is
  single-process.
- No allocation-level counters exist in this harness beyond the `io-metrics`
  byte/nanos counters; "allocations if available" from the plan therefore
  resolves to those counters plus peak RSS.
- The comparison to `f61e0853` spans a compiler change (1.85.0 → 1.98.0), a
  dependency change (polars 0.46 → 0.55.2 with a new expression/stream/OOC
  layer) and the A1/A2 adapters. Any difference is therefore an observation
  about the whole stack, not about any single change.
- Engine cases in this harness run under a **current-thread** Tokio runtime
  (`#[tokio::test]` default), so they exercise the A2 adapter's off-runtime
  path; production executes the same lowering inline on a multi-threaded
  runtime. The measured engine p50s are not worse than the reference, so the
  extra thread hop is immaterial at this granularity — but the harness does not
  measure the production inline path.
- The reference baseline's engine cases ran under the same current-thread
  harness on 0.46, where the runtime flavour was irrelevant to Polars; the
  comparison therefore stays meaningful while the flavour difference is
  disclosed.
- Only one measurement pass per case was taken (the harness's internal reps are
  the sample). No day-to-day variance study is included.
