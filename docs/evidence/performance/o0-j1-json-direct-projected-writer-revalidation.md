# O0-J1: Revalidation of the JSON direct projected writer on post-H3 main

- Version: 1.0.0
- Date: 2026-09-05
- Issue: #283 ([O0-J1] Revalidate JSON direct projected writer on post-H3 main)
- Measured head: `f61e0853b67ff5ca7bedb0bddb707befb922baff` ([H3] Product Release
  Gate = dispatch base = origin/main at dispatch time). Every measurement and
  both test suites ran at this exact commit; no production commit landed
  between base and measurement. Branch: `agent/issue-283-o0-j1-json-direct-writer`.
- Reference baseline: O0-B1 (#282). The feature-OFF arm of this note IS the
  default production path measured at the identical exact head, so every
  OFF number below is an exact-head baseline; absolute values should be
  cross-read against #282 (PR was not yet open when this note was finalized;
  both tasks measure at base `f61e085…`).
- Scope: measurement/compatibility only. The production default, routing,
  public contracts, dependencies, persistence formats and JSON parser
  architecture are unchanged. The only repo text change is the stale
  feature-status comment (see section 8), permitted by the issue.

## 1. Summary

| Gate | Result |
| --- | --- |
| Semantic parity (default vs `json-direct-projected-writer`) | **PASS** on every probe: canonical value digests identical cross-mode on all 6 parity fixtures (incl. 100k-row temporal), Arrow schemas identical, error categories/messages identical on 8 malformed/drift fixtures, repeated-run digests identical (determinism), committed 26-test differential oracle suite passes identically in both modes |
| Wall time (P50, 5+ runs, flock-serialized) | Direct projected path is faster on every JSON-shape cell: **−41%** (narrow full), **−41%** (100-col near-full), **−36%** (top-level array), **−15%** (temporal-heavy), **−32%** (nested); **−3% to −6%** on a 5-of-100 narrow projection (ranges disjoint); **within noise** on the escape-heavy fixture |
| Peak RSS (VmHWM) | No material regression: −8% to −5% on temporal/escape, ±2 MB on the rest; largest measured increase +1.9 MB (~8%) on narrow/sparse, with a 36–41% end-to-end wall benefit |
| Old #151 blocker | **Obsolete** for this path (see section 8) |
| Recommendation | **GO** for a separate future productionization task (section 9) |

## 2. What was measured

The private cargo feature `json-direct-projected-writer`
(`backend/crates/stillflow-connector-local-tabular/Cargo.toml`) replaces only
the selected-field generic `serde_json::Value` tree plus projected
`Map<String, Value>` reconstruction of the default JSON path
(`read.rs::parse_projected_object`) with borrowed raw-slice capture and
byte-concatenation assembly (`direct_projected.rs::ProjectedRowAssembler`).
Everything else — framing (`json_stream.rs`), typed validation
(`ValidateFieldSeed`), the retained Polars `JsonReader` second parse, the
`json_ingest_schema` Int64 boundary and `cast_ingested_timestamp` restore from
PR #225 — is shared and unchanged. Both arms were built from the same source
tree; the only difference is the cargo feature.

- OFF arm: default build (exact production path).
- ON arm: `--features json-direct-projected-writer` (never enabled through
  configuration; measurement-only).
- Counter passes additionally enable `--features io-metrics` (additive
  relaxed-atomic counters + dump side channel; measurement-only).

## 3. Machine and concurrency policy

- 6 vCPU (12th Gen Intel Core i3-12100F), 11 GiB RAM, WSL2 kernel
  6.18.33.2-microsoft-standard-WSL2, Rust 1.85.0 (pinned by rust-toolchain.toml).
- Shared `CARGO_TARGET_DIR=/home/owl/.cargo-o0-target` for every cargo command;
  builds serialize via cargo's own lock.
- **Every timed run** (warm-up and sample) was executed as
  `flock /tmp/stillflow-o0-measure.lock -c '<cmd>'`, the same cross-conversation
  lock used by the sibling O0 tasks running in parallel on this machine.
- One fresh process per sample: each sample's `/proc/self/status` `VmHWM` is
  that sample's true peak (peak RSS methodology per issue; `/usr/bin/time -v`
  is not installed on this machine, `/proc` sampling is the alternative the
  issue allows).
- >= 5 timed samples per cell/mode after 1 untimed warm-up; P50/P95 and
  inter-run spread reported. A win is only called when the inter-run value
  ranges of OFF and ON are disjoint or the delta is far larger than the
  combined spread.

## 4. Fixture matrix (issue scope -> fixtures)

| # | Required shape | Fixture (bytes, SHA-256 prefix) | Used for |
| --- | --- | --- | --- |
| 1 | NDJSON | `e24_narrow_10x100k.ndjson` (14,850,196 B, `6a2f9441`); `o0j1_temporal_8x100k.ndjson` (25,710,121 B, `790e8a65`); `o0j1_longutf8_3x20k.ndjson` (13,012,130 B, `607caef1`) | perf + parity + counters |
| 2 | Top-level JSON arrays | `o0j1_array_40x50k.json` (29,622,365 B, `ed4f1be4`); `o0j1_parity_array.json` (523,326 B, `cac920a8`) | perf + parity |
| 3 | Narrow projection from wide input | e24 sparse cell: 5 of 100 columns of `e24_primary_100x100k.ndjson` (148,328,217 B, `27053ee3`); `pmixed_narrow`: 3 of 16 of `o0j1_parity_mixed.ndjson` (519,327 B, `7de716c8`) | perf + parity |
| 4 | Wide / near-full projection | e24 primary cell (all 100 columns); e24 narrow cell (all 10); `pmixed` (all 16) | perf + parity |
| 5 | Long UTF-8 / nested-or-variable values | `o0j1_longutf8_3x20k.ndjson` (multibyte CJK/emoji/escape blobs ~1.2 KiB + nested struct); `o0j1_parity_nested.ndjson` (296,273 B, `ec7fca98`) incl. duplicate nested keys, whitespace-inside-subtree, wide integer literals, pre-1970 timestamps; `e24_escape_50k.ndjson` (18,086,992 B, `91faf60d`); `e24_nested_20k.ndjson` (953,538 B, `2e07857b`) | perf + parity |
| 6 | Declared timestamp columns: ms/us/ns, timezone-bearing, pre-1970, nulls, malformed temporal | `o0j1_temporal_8x100k.ndjson`: 8 declared columns — `id Int64`, `ts_ms Timestamp(ms)`, `ts_us Timestamp(us)`, `ts_ns Timestamp(ns)`, `ts_tz Timestamp(ms, TZ=UTC)` with offsets `Z/+05:30/-08:00/+00:00/-03:00`, `d32 Date32`, `label Utf8(CJK)`, `val Float64`; 1 in 5 rows pre-1970 (1969-12), 1 in 11 rows null temporal group (never the inference sentinel row), 1 in 13 rows null `val`; malformed temporal covered by `e_malformed_temporal` | perf + temporal parity + error timing |
| 7 | Malformed JSON and schema-drift/error cases | `e_malformed_json`, `e_schema_drift`, `e_malformed_temporal`, `e_unknown_field`, `e_duplicate_field`, `e_missing_required` (NDJSON, row 2), `e_wrong_array_element`, `e_truncated_array` (top-level array framing) | error category/message/phase + timing |

All fixtures are byte-deterministic pure functions of (row, column) via integer
LCG/format arithmetic — no RNG crate, no timestamps, no filesystem order.

## 5. Exact commands

Environment (every command):

```bash
export CARGO_TARGET_DIR=/home/owl/.cargo-o0-target
cd /home/owl/stillflow-o0j1/backend
```

Test suites (acceptance, both modes):

```bash
cargo test -p stillflow-connector-local-tabular
cargo test -p stillflow-connector-local-tabular --features json-direct-projected-writer
cargo fmt --check
```

Committed e24 evidence harness cells (OFF/ON; release; one fresh process per
sample; `FEAT` empty for OFF, `--features json-direct-projected-writer` for ON):

```bash
flock /tmp/stillflow-o0-measure.lock -c \
  "cargo test -p stillflow-connector-local-tabular --release $FEAT \
   --test e24_json_a2_prod_evidence -- <cell> --ignored --nocapture"
# cells: e24_cell_narrow e24_cell_primary e24_cell_sparse
#        e24_cell_nested e24_cell_escape e24_mem_primary_256 e24_mem_primary_4096
```

External O0-J1 harness (cells outside the committed harness; source in
Appendix A; fixtures under `O0J1_FIXTURES`):

```bash
flock /tmp/stillflow-o0-measure.lock -c \
  "O0J1_FIXTURES=/tmp/o0j1-fixtures <bin> run <cell>"
# perf cells:    array40x50k temporal100k longutf8
# parity cells:  pmixed pmixed_narrow parray pnested pnested_array ptemporal
# error cells:   e_malformed_json e_schema_drift e_malformed_temporal
#                e_unknown_field e_duplicate_field e_missing_required
#                e_wrong_array_element e_truncated_array
# counter cells: counters_array40x50k counters_temporal100k counters_longutf8
#                (bin built with --features [direct,]io-metrics)
```

`<bin>` is the harness built twice from one source: OFF = default features,
ON = `--features direct` (which maps to
`stillflow-connector-local-tabular/json-direct-projected-writer`).

## 6. Correctness / parity matrix

### 6.1 Committed differential oracle suite

`tests/direct_projected_writer.rs` (26 tests) asserts absolute observable
behavior — final tabular values from public envelopes, error categories,
stable messages, earliest failing row — on identical fixture bytes, compiled
into both modes. Results at the measured head:

- OFF: 26 passed / 0 failed. ON: 26 passed / 0 failed.
- Temporal coverage includes `timestamps_use_declared_precision_and_timezone_epoch`
  (us/ns units, pre-1970 boundary `-1`, TZ `+02:00` epoch semantics) and
  `temporal_forms_accepted_consistently` (declared ms epoch values), plus
  nested duplicate-key last-wins, raw control bytes, list/struct nullability,
  bounded byte/row laws and cancellation surfaces.
- Full crate suite in both modes: 23 lib + 26 oracle + 11 `local_tabular` +
  1 `memory_bound` tests, all passing, identical counts in both arms.

### 6.2 Canonical value digests (public envelope payloads)

FNV-1a over a canonical rendering of every envelope payload (column names,
metadata-free Arrow types, nullability, and every scalar/list/struct value),
printed by the external harness. All cells ran 3x per mode; every cell's
digest was identical across runs and across modes.

| Parity cell | Rows | OFF digest | ON digest | Cross-mode |
| --- | --- | --- | --- | --- |
| `pmixed` (16 mixed scalar cols, escapes/unicode/negatives) | 2,000 | `27fc11e5c4fdff0d` | `27fc11e5c4fdff0d` | equal |
| `parray` (same rows, top-level JSON array, whitespace between elements) | 2,000 | `27fc11e5c4fdff0d` | `27fc11e5c4fdff0d` | equal (= `pmixed`: array framing is value-identical to NDJSON) |
| `pmixed_narrow` (3-of-16 projection) | 2,000 | `9c2144c22dacad80` | `9c2144c22dacad80` | equal |
| `pnested` (lists w/ inner nulls + empty, structs w/ shuffled + duplicate keys last-wins, wide ints on Float64, us timestamps, TZ timestamps) | 2,000 | `8bbabce7ccef71bc` | `8bbabce7ccef71bc` | equal |
| `pnested_array` (same rows under array framing with raw newlines inside subtrees — exercises the direct path's control-byte canonicalization fallback) | 2,000 | `8bbabce7ccef71bc` | `8bbabce7ccef71bc` | equal (= `pnested`: canonicalization reproduces generic DOM semantics byte-for-byte at the value level) |
| `ptemporal` (declared ms/us/ns + TZ + Date32 + nulls + pre-1970) | 100,000 | `d3f7576ea949f8ba` | `d3f7576ea949f8ba` | equal |

Output schemas (column order, identities, logical types, nullability) are
printed alongside each digest and were identical cross-mode in every cell
(e.g. `ptemporal`:
`id Int64; ts_ms Timestamp(ms,None); ts_us Timestamp(us,None);
ts_ns Timestamp(ns,None); ts_tz Timestamp(ms,Some("UTC")); d32 Date32;
label Utf8; val Float64`).

### 6.3 Error surface parity

| Cell | Failure phase | Category (OFF = ON) | Stable message (OFF = ON) |
| --- | --- | --- | --- |
| `e_malformed_json` (truncated JSON row) | mid-stream, first batch | InvalidData | `JSON row does not match the established schema at row 2` |
| `e_schema_drift` (Int64 col receives string) | mid-stream, first batch | SchemaDrift | same |
| `e_malformed_temporal` (declared ms col receives `not-a-timestamp`) | mid-stream, first batch | SchemaDrift | same |
| `e_unknown_field` (field outside established schema) | mid-stream, first batch | SchemaDrift | same |
| `e_duplicate_field` (duplicate top-level key) | mid-stream, first batch | SchemaDrift | same |
| `e_missing_required` (required field absent) | mid-stream, first batch | SchemaDrift | same |
| `e_wrong_array_element` (string inside top-level array) | framing, first batch | InvalidData | `every JSON array element must be an object` |
| `e_truncated_array` (array ends mid-object) | framing, first batch | InvalidData | `JSON object ended before its closing brace` |

Rows/envelopes emitted before the terminal error were identical cross-mode
(0/0 everywhere; the fixtures fail inside the first batch). 3 runs per
cell/mode, every run identical.

### 6.4 Determinism

Every parity digest cell ran 3x per mode: all digests constant. Every error
cell ran 3x per mode: categories, messages, and rows-before-failure constant.
The retained Polars reader and the assembled-byte path are deterministic
functions of the fixture bytes, confirmed empirically at this head.

## 7. Performance

### 7.1 Committed e24 harness cells (5 timed runs per cell/mode)

| Cell (fixture, projection) | OFF P50/P95 (ms) | ON P50/P95 (ms) | P50 delta | Noise (OFF/ON spread, disjoint ranges?) |
| --- | --- | --- | --- | --- |
| narrow (10 cols x 100k, full) | 449.0 / 498.5 | 263.2 / 274.2 | **−41.4%** | 12.6% / 5.2%, disjoint |
| primary (100 cols x 100k, near-full) | 7184.3 / 7567.8 | 4250.5 / 4336.4 | **−40.8%** | 9.5% / 3.1%, disjoint |
| sparse (5 of 100 from wide) | 3721.3 / 3880.4 | 3595.3 / 3633.1 | **−3.4%** (P95 −6.4%) | 5.9% / 2.3%, ranges disjoint (OFF min 3661.9 > ON max 3633.1) |
| nested (20k, list/struct) | 44.7 / 55.5 | 30.6 / 38.8 | **−31.6%** | 35.3% / 30.8% (noisy fixture), ranges disjoint (OFF min 39.7 > ON max 38.8) |
| escape (50k, escape/unicode-heavy strings) | 278.4 / 306.7 | 272.8 / 280.3 | −2.0% | 12.3% / 5.9%, ranges overlap → **within noise** |
| mem256 (primary, batch 256) | 7609.9 / 7759.9 | 4874.0 / 5001.7 | **−35.9%** | 4.8% / 4.1%, disjoint |
| mem4096 (primary, batch 4096) | 7037.2 / 7326.3 | 4179.3 / 4669.1 | **−40.6%** | 6.0% / 12.4%, disjoint |

### 7.2 External harness cells (5 timed runs per cell/mode)

| Cell | OFF P50/P95 (ms) | ON P50/P95 (ms) | P50 delta | Noise |
| --- | --- | --- | --- | --- |
| array40x50k (top-level array, 40 cols x 50k, full) | 1305.0 / 1322.9 | 833.4 / 841.4 | **−36.1%** | 4.4% / 5.2%, disjoint |
| temporal100k (8 declared temporal-ish cols x 100k, full) | 598.7 / 677.4 | 512.0 / 531.1 | **−14.5%** | 17.6% / 5.0%, ranges disjoint (OFF min 572.0 > ON max 531.1) |
| longutf8 (long multibyte blobs + nested struct, 20k) | 117.6 / 140.9 | 117.7 / 124.4 | 0.0% | 24.6% / 12.2%, ranges overlap → **within noise** |

Reading: the win concentrates exactly where the replaced work lives — the
selected-field `Value` DOM construction and `Map` reconstruction plus compact
re-serialization (wide/full projections, top-level arrays, nested subtrees).
It disappears on escape-heavy text (where the retained Polars parse and the
canonicalization fallbacks dominate) and shrinks on very narrow projections
(where non-selected typed streaming dominates).

### 7.3 Peak RSS (VmHWM, median of 5 fresh-process samples, KiB)

| Cell | OFF | ON | delta |
| --- | --- | --- | --- |
| narrow | 25,172 | 27,056 | +7.5% (+1.9 MB) |
| primary | 41,032 | 42,564 | +3.7% (+1.5 MB) |
| sparse | 24,632 | 26,492 | +7.5% (+1.9 MB) |
| nested | 21,372 | 22,992 | +7.6% (+1.6 MB) |
| escape | 30,360 | 28,952 | −4.6% |
| mem256 | 24,004 | 24,604 | +2.5% |
| mem4096 | 41,444 | 42,644 | +2.9% |
| array40x50k | 31,180 | 28,876 | −7.4% |
| temporal100k | 27,304 | 27,044 | −1.0% (ranges overlap) |
| longutf8 | 34,628 | 35,180 | +1.6% (ranges overlap) |

No material memory regression: the largest consistent increase is ~1.9 MB
(~8%) on small-RSS fixtures, against a 36–41% end-to-end wall benefit on the
same cells, and several fixtures measure lower. This does not trigger the
issue's resource-bound stop conditions (batch-size invariance held: mem256
remains the lowest-RSS configuration in both arms, and the committed
`memory_bound` law test passes in both modes).

### 7.4 Logical bytes and parser-invocation counters (`io-metrics`)

One run per cell/mode (counters are exact and additive; the dump is written
when the read stream drops):

| Counter | array40x50k OFF/ON | temporal100k OFF/ON | longutf8 OFF/ON |
| --- | --- | --- | --- |
| `json_handle_bytes` (logical bytes through file handle) | 29,622,365 / 29,622,365 | 25,710,121 / 25,710,121 | 13,012,130 / 13,012,130 |
| `json_framed_bytes` (framed raw rows) | 29,573,402 / 29,573,402 | 25,610,563 / 25,610,563 | 12,992,892 / 12,992,892 |
| `json_reencode_bytes` (assembled NDJSON fed to Polars) | 29,247,364 / 29,622,364 (+1.3%) | 20,708,113 / 20,728,305 (+0.1%) | 13,012,130 / 13,012,130 (0.0%) |
| `json_framed_rows` | 50,000 / 50,000 | 100,000 / 100,000 | 20,000 / 20,000 |
| `json_polars_decode_invocations` | 13 / 13 | 25 / 25 | 5 / 5 |
| `inference_phase_bytes` | 16,777,216 / 16,777,216 | 16,777,216 / 16,777,216 | 16,777,216 / 16,777,216 |

Interpretation: identical logical I/O, identical framing work, identical
Polars parse passes. The assembled bytes differ only where the generic path's
DOM re-serialization normalizes spellings the raw path preserves (e.g.
`40.000` -> `40.0` on exact-multiple floats), which decodes identically (and
is covered by the digest parity above). The measured win is therefore
allocation/CPU on the serde side, not fewer bytes or fewer downstream parses.

## 8. Is the old #151 blocker obsolete?

The `Cargo.toml` feature comment (and the `lib.rs` feature-gate comment) state
that default enablement is "blocked on the #151 temporal upstream boundary
(`TIMESTAMP_ROOT_CAUSE_POLARS_UPSTREAM`)": the direct projected path keeps the
shared Polars `JsonReader` and adds no timestamp compensation.

Verdict: **obsolete for this path** — the specific blocking condition it
describes no longer exists.

- Issue #151 ("retained Polars JsonReader decodes ISO timestamps with a x1000
  scale shift, baseline both modes") is CLOSED; the fix is connector-side and
  landed in PR #225 (`1ae4faa410374bb7fba0b34e6b7a5d80a022415c`, merged):
  declared timestamps are normalized to unit-scaled epoch integers
  (`normalize_json_temporal_value`) and the retained reader receives an Int64
  ingest schema (`json_ingest_schema`) with a `cast_ingested_timestamp`
  restore — no upstream Polars behavior is trusted for temporal text anymore.
- The compensation is applied on BOTH paths: the generic path normalizes the
  selected `Value` in place; the direct projected path routes every
  timestamp-containing capture through the same normalization via its
  canonicalization fallback (`contains_timestamp` ->
  `canonical_captured_value` -> `normalize_json_temporal_value`).
- Measured confirmation at this head: the `ptemporal` digest (ms/us/ns units,
  TZ-bearing offsets incl. non-UTC, pre-1970 values, interleaved nulls) is
  identical across modes; the committed oracle tests
  `timestamps_use_declared_precision_and_timezone_epoch` and
  `temporal_forms_accepted_consistently` pass in both modes; malformed
  temporal values produce the same SchemaDrift category/message at the same
  row in both modes.

"Obsolete" is deliberately narrower than "the feature is now safe to enable
by default": the comment's remaining, still-true statements are that the path
retains the shared reader and adds no compensation of its own. What changed
is that the compensation now exists upstream of the reader on both paths, so
the temporal boundary can no longer diverge between them. Default enablement
remains a productionization decision outside this task's scope (section 9).

Per the issue's allowance, the stale comment text was updated (comment-only
diff in `Cargo.toml` and `lib.rs`); no code, default, routing, or contract
change.

## 9. Recommendation: GO (for a separate future productionization task)

**GO** — recommend a separate, dedicated productionization task to consider
default enablement (or targeted routing) of `json-direct-projected-writer`,
carrying this note as evidence:

1. Semantic parity gate: every probe passes (section 6) — column
   order/identities, types, nullability, timestamp units/timezone/pre-1970
   epoch values, malformed behavior, schema drift, bounded laws (committed
   suites), and determinism are indistinguishable between arms.
2. Performance: 0% (within noise) to 41% end-to-end wall improvement
   depending on shape; improvement is far beyond noise on the wide/full
   projection and array cells, modest-but-consistent on narrow projections,
   and never a regression outside noise on any measured shape.
3. Memory: no material regression (section 7.3); batch-size invariance held.
4. Counters confirm the mechanism: same logical bytes, same framing, same
   Polars decode passes; only the serde-side work is removed.

Stop-condition audit: none triggered — no timestamp/timezone mismatch, no
different malformed-data exposure, no resource-bound regression, and the win
is not within noise only. A productionization task should still add
fuzz/differential coverage beyond this matrix and re-measure on target
hardware before flipping any default; this note does not change any default.

## 10. Deviations

- `/usr/bin/time -v` is not installed in this environment; peak RSS uses
  `/proc/self/status` `VmHWM` sampling (the issue permits either).
- The measurement harness for the new shapes (top-level arrays, declared
  temporal columns, long UTF-8, parity digests, error cells, counter dumps)
  lives outside the repo (`/home/owl/o0j1-harness`); its full source is
  reproduced in Appendix A so the cells are reproducible. The committed
  e24-JSON-A2 evidence harness (`tests/e24_json_a2_prod_evidence.rs`) was
  reused verbatim for the original five perf cells and the two batch-size
  memory cells.
- PR #282 (O0-B1) had not been opened when this note was finalized; the
  feature-OFF arm here is the exact-head default-path baseline, so all
  deltas are self-contained and cross-readable against #282 at
  `f61e0853b67ff5ca7bedb0bddb707befb922baff`.
- The `pmixed` digest initially appeared to differ cross-mode because an
  earlier harness revision hashed Debug-rendered Arrow types (which embed
  per-run random struct member ids); the final harness hashes metadata-free
  type tags and the mismatch disappeared. Recorded for transparency; the
  reported digests are from the final harness.

## 11. Acceptance criteria checklist

- [x] Default and feature-enabled test suites pass on the exact measured head
      (61 tests each: 23 lib + 26 differential oracle + 11 + 1; plus
      `cargo fmt --check` clean).
- [x] Temporal parity covers ms/us/ns, timezone, pre-1970, null and malformed
      cases (section 6.2 `ptemporal`, section 6.1 oracle tests, section 6.3
      `e_malformed_temporal`).
- [x] JSON/NDJSON and projection-width variants are measured (sections 7.1,
      7.2: NDJSON, top-level arrays, full/near-full/narrow projections).
- [x] P50/P95 and peak RSS are reported with noise bounds (sections 7.1-7.3).
- [x] Parser/logical-I/O counters are included where available (section 7.4).
- [x] No production default or API/schema/persistence contract changes
      (feature remains opt-in; only comment/doc text changed).
- [x] Final recommendation is evidence-based and explicitly separate from
      production implementation (section 9: GO for a separate task).

## Appendix A: external harness source

The harness is a standalone cargo bin crate depending on the workspace crate
by path; its features map to the private connector features (`direct` ->
`json-direct-projected-writer`, `metrics` -> `io-metrics`). The complete
source at the measured revision follows (doc-only reproduction; nothing of
this appendix is compiled by the workspace).

### A.1 `Cargo.toml`

```toml
[package]
name = "o0j1-harness"
version = "0.1.0"
edition = "2021"

[dependencies]
arrow-array = "59"
arrow-schema = "59"
futures = "0.3"
serde_json = { version = "1", features = ["preserve_order"] }
stillflow-connector-local-tabular = { path = "/home/owl/stillflow-o0j1/backend/crates/stillflow-connector-local-tabular" }
stillflow-connectors = { path = "/home/owl/stillflow-o0j1/backend/crates/stillflow-connectors" }
stillflow-core = { path = "/home/owl/stillflow-o0j1/backend/crates/stillflow-core" }

[features]
default = []
# O0-J1 mode "on": the private direct projected writer feature.
direct = ["stillflow-connector-local-tabular/json-direct-projected-writer"]
# Measurement-only parser/byte counters (additive, additive side-channel dump).
metrics = ["stillflow-connector-local-tabular/io-metrics"]

[profile.release]
debug = false
```

### A.2 `src/main.rs`

```rust
//! O0-J1 measurement harness for issue #283 (external, not part of the repo).
//!
//! Reuses the committed E24-JSON-A2 evidence methodology (fresh process per
//! sample, deterministic fixtures, /proc/self/status VmHWM peak) and adds:
//!   * top-level JSON array and temporal/long-UTF-8 fixture cells;
//!   * canonical value digests of the public envelope payloads (parity
//!     witness across modes and across repeated runs);
//!   * error cells (category / stable message / rows-before / timing);
//!   * io-metrics counter dumps (with the `metrics` feature).
//!
//! Sample lines:
//!   O0J1SAMPLE cell=<c> mode=<off|on> rows=<n> envelopes=<n> elapsed_ns=<n> rss_start_kb=<n> rss_end_kb=<n> vmhwm_kb=<n>
//!   O0J1DIGEST cell=<c> mode=<off|on> rows=<n> digest=<16 hex> schema=<canonical schema string>
//!   O0J1ERROR cell=<c> mode=<off|on> category=<cat> rows=<n> envelopes=<n> elapsed_ns=<n> message=<stable message>
//!   O0J1COUNTERS cell=<c> mode=<off|on> rows=<n> elapsed_ns=<n> <label>=<u64> ...
//!   O0J1FIXTURE <name> <bytes>

use std::io::{BufWriter, Write};
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, ListArray,
    NullArray, RecordBatch, StringArray, StructArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, TimeUnit as ArrowTimeUnit};
use futures::StreamExt;
use stillflow_connector_local_tabular::LocalTabularConnector;
use stillflow_connectors::{ConnectorRegistry, SourceConnectorRef};
use stillflow_core::{
    ColumnId, ConnectorKind, CredentialRef, DiscoverRequest, ErrorCategory, InspectRequest,
    LogicalField, LogicalSchema, LogicalType, ReadRequest, RequestContext, SourceConnection,
    TimeUnit,
};

#[cfg(feature = "direct")]
const MODE: &str = "on";
#[cfg(not(feature = "direct"))]
const MODE: &str = "off";

const BATCH: usize = 4_096;

// ---------------------------------------------------------------------------
// Deterministic fixture generation (pure function of (row, col), like E24).
// ---------------------------------------------------------------------------

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }
}

fn column_kind(index: usize) -> &'static str {
    match index % 4 {
        0 => "int",
        1 => "utf8",
        2 => "float",
        _ => "bool",
    }
}

fn column_name(index: usize) -> String {
    format!("c{index:0>3}")
}

fn write_scalar(row: usize, index: usize, sink: &mut String) {
    match column_kind(index) {
        "int" => sink.push_str(&((row * 31 + index * 7) % 90_000 + 1).to_string()),
        "float" => sink.push_str(&format!(
            "{:.3}",
            ((row * 13 + index * 5) % 100_000) as f64 / 8.0 + 0.125
        )),
        "bool" => sink.push_str(if (row + index) % 2 == 0 { "true" } else { "false" }),
        _ => sink.push_str(&format!("\"v{}x{}\"", row % 9_973, index)),
    }
}

fn write_wide_row(row: usize, cols: usize, sink: &mut String) {
    sink.push('{');
    for index in 0..cols {
        if index > 0 {
            sink.push(',');
        }
        sink.push_str(&format!("\"{}\":", column_name(index)));
        write_scalar(row, index, sink);
    }
    sink.push('}');
}

/// 40 cols x 50k rows written as ONE top-level JSON array (compact).
fn generate_array(path: &std::path::Path, rows: usize, cols: usize) -> std::io::Result<u64> {
    let file = std::fs::File::create(path)?;
    let mut out = BufWriter::with_capacity(1 << 20, file);
    let mut row = String::with_capacity(1 << 12);
    out.write_all(b"[")?;
    for r in 0..rows {
        if r > 0 {
            out.write_all(b",")?;
        }
        row.clear();
        write_wide_row(r, cols, &mut row);
        out.write_all(row.as_bytes())?;
    }
    out.write_all(b"]")?;
    out.flush()?;
    Ok(std::fs::metadata(path)?.len())
}

/// One temporal row: 8 declared columns (id Int64, ts_ms, ts_us, ts_ns,
/// ts_tz[TZ=UTC ms], d32 Date32, label Utf8, val Float64) with pre-1970 rows,
/// timezone-bearing values with varying offsets, ms/us/ns precision, and
/// interleaved nulls (never on row 0, the inference sentinel).
fn temporal_row(row: usize) -> String {
    let pre1970 = row % 5 == 3;
    let year = if pre1970 { 1969 } else { 2024 + (row % 2) };
    let month = if pre1970 { 12 } else { 1 + row % 12 };
    let day = 1 + (row * 7) % 28;
    let (hh, mm, ss) = (row % 24, (row * 3) % 60, (row * 11) % 60);
    let millis = (row * 137) % 1_000;
    let micros = (row * 1_037) % 1_000_000;
    let nanos = (row * 10_003) % 1_000_000_000;
    let tz_offsets = ["Z", "+05:30", "-08:00", "+00:00", "-03:00"];
    let tz = tz_offsets[row % tz_offsets.len()];
    let date = format!("{year:04}-{month:02}-{day:02}");
    let clock_ms = format!("{hh:02}:{mm:02}:{ss:02}.{millis:03}");
    let label_len = 8 + row % 24;
    let label: String = (0..label_len)
        .map(|i| char::from_u32(0x4e00 + ((row * 31 + i * 7) % 500) as u32).unwrap_or('あ'))
        .collect();
    let val = format!("{:.4}", ((row * 17) % 1_000_000) as f64 / 64.0);

    // Null pattern: one row in eleven nulls the temporal group (never row 0);
    // d32 nulls one row in seven (never row 0); val nulls one row in thirteen.
    let temporal_null = row % 11 == 7 && row != 0;
    let d32_null = row % 7 == 4 && row != 0;
    let val_null = row % 13 == 5 && row != 0;
    format!(
        "{{\"id\":{row},\
         \"ts_ms\":{},\
         \"ts_us\":{},\
         \"ts_ns\":{},\
         \"ts_tz\":{},\
         \"d32\":{},\
         \"label\":\"{label}\",\
         \"val\":{}}}",
        if temporal_null {
            "null".into()
        } else {
            format!("\"{date}T{clock_ms}\"")
        },
        if temporal_null {
            "null".into()
        } else {
            format!("\"{date}T{hh:02}:{mm:02}:{ss:02}.{micros:06}\"")
        },
        if temporal_null {
            "null".into()
        } else {
            format!("\"{date}T{hh:02}:{mm:02}:{ss:02}.{nanos:09}\"")
        },
        if temporal_null {
            "null".into()
        } else {
            format!("\"{date}T{clock_ms}{tz}\"")
        },
        if d32_null {
            "null".into()
        } else {
            format!("\"{date}\"")
        },
        if val_null {
            "null".into()
        } else {
            val
        },
    )
}

/// Writes the NDJSON temporal fixture.
fn generate_temporal(path: &std::path::Path, rows: usize) -> std::io::Result<u64> {
    let file = std::fs::File::create(path)?;
    let mut out = BufWriter::with_capacity(1 << 20, file);
    for row in 0..rows {
        out.write_all(temporal_row(row).as_bytes())?;
        out.write_all(b"\n")?;
    }
    out.flush()?;
    Ok(std::fs::metadata(path)?.len())
}

/// Long UTF-8 / variable-value fixture: 3 cols (id, blob ~1.2 KiB multibyte
/// text with escapes, nested struct with variable string). 20k rows.
fn generate_longutf8(path: &std::path::Path, rows: usize) -> std::io::Result<u64> {
    let file = std::fs::File::create(path)?;
    let mut out = BufWriter::with_capacity(1 << 20, file);
    for row in 0..rows {
        let mut seed =
            Lcg(0x9E37_79B9_7F4A_7C15 ^ (row as u64).wrapping_mul(0xFF51_AFD7_ED55_8CCD));
        let mut blob = String::with_capacity(1_400);
        let units = 90 + (row % 5) * 30;
        for _ in 0..units {
            match seed.next() % 8 {
                0 => blob.push_str("日本語"),
                1 => blob.push_str("é"),
                2 => blob.push_str("\\\"q"),
                3 => blob.push_str("\\\\"),
                4 => blob.push_str("\\n"),
                5 => blob.push_str("🎉"),
                6 => blob.push_str("data"),
                _ => blob.push_str(&format!("{}", seed.next() % 1_000)),
            }
        }
        let y_len = 4 + row % 40;
        let y: String = (0..y_len)
            .map(|i| char::from_u32(0x00E9 + ((row + i) % 200) as u32).unwrap_or('x'))
            .collect();
        out.write_all(
            format!(
                r#"{{"id":{},"blob":"{}","nested":{{"x":{},"y":"{}"}}}}"#,
                row as i64,
                blob,
                (row * 7) % 5_000,
                y
            )
            .as_bytes(),
        )?;
        out.write_all(b"\n")?;
    }
    out.flush()?;
    Ok(std::fs::metadata(path)?.len())
}

// ---------------------------------------------------------------------------
// Parity fixtures (small, deterministic; digest witness).
// ---------------------------------------------------------------------------

/// 16 mixed scalar columns, 2k rows; includes escapes, unicode, negatives.
fn mixed_row(row: usize) -> String {
    let mut parts = Vec::with_capacity(16);
    for index in 0..16 {
        let name = format!("\"{}\":", column_name(index));
        let value = match column_kind(index) {
            "int" => {
                let v = (row as i64 * 31 + index as i64 * 7) % 90_001 - 45_000;
                format!("{name}{v}")
            }
            "utf8" => {
                let v = if row % 9 == 4 {
                    "quote\\\"back\\\\slash\\u00e9\\ud83c\\udf89 newline\\n\\u65e5\\u672c\\u8a9e"
                } else if row % 9 == 7 {
                    "plain"
                } else {
                    "v5837"
                };
                format!("{name}\"{v}\"")
            }
            "float" => {
                let v = ((row * 13 + index * 5) % 100_000) as f64 / 8.0 - 6_250.0 + 0.125;
                format!("{name}{v:.3}")
            }
            _ => format!(
                "{name}{}",
                if (row + index) % 2 == 0 { "true" } else { "false" }
            ),
        };
        parts.push(value);
    }
    format!("{{{}}}", parts.join(","))
}

/// Nested/canonicalization-stress row (NDJSON-safe: no raw control bytes can
/// appear inside an NDJSON line): structs with shuffled and DUPLICATE keys,
/// wide integer literals on Float64, timestamp members, timezone members.
fn nested_row(row: usize) -> String {
    let st: String = if row % 7 == 3 {
        r#"{"x":1,"y":"old","x":2,"y":"new"}"#.to_string() // duplicate nested keys, last wins
    } else if row % 7 == 5 {
        r#"{ "x" : 10 , "y" : "spaced" }"#.to_string() // interior whitespace (no newline)
    } else {
        format!(r#"{{"x":{},"y":"y{}"}}"#, row, row % 13)
    };
    let li: String = if row % 11 == 9 {
        "[1, 2, null, 3]".to_string()
    } else if row % 11 == 5 {
        "[]".to_string()
    } else {
        format!("[{},{},null,{}]", row, row * 2, row * 3)
    };
    let wide: String = if row % 5 == 2 {
        "1000000000000000000000000000000".to_string() // exceeds i64: float canonicalization path
    } else {
        format!("{:.3}", ((row * 29) % 500_000) as f64 / 16.0)
    };
    let ts_us: String = if row % 4 == 1 {
        "null".to_string()
    } else if row % 8 == 6 {
        "\"1969-12-31T23:59:59.999999\"".to_string() // pre-1970 boundary
    } else {
        format!("\"2024-0{}-1{}T0{}:17:45.123456\"", 1 + row % 9, 1 + row % 9, row % 10)
    };
    let ts_tz: String = if row % 4 == 3 {
        "null".to_string()
    } else {
        "\"2024-06-01T12:00:00.500+02:00\"".to_string()
    };
    format!(
        r#"{{"id":{row},"li":{li},"st":{st},"wide":{wide},"ts_us":{ts_us},"ts_tz":{ts_tz}}}"#
    )
}

/// Array-format nested parity row: raw newlines/spaces inside List/Struct
/// subtrees (legal under top-level array framing) exercise the direct path's
/// control-byte canonicalization fallback; duplicate nested keys and wide
/// integers exercise the other two fallbacks.
fn nested_array_row(row: usize) -> String {
    let st: String = if row % 7 == 3 {
        r#"{"x":1,"y":"old","x":2,"y":"new"}"#.to_string()
    } else if row % 7 == 5 {
        "{ \"x\" : 10 ,\n \"y\" : \"spaced\" }".to_string() // raw newline inside subtree
    } else {
        format!(r#"{{"x":{},"y":"y{}"}}"#, row, row % 13)
    };
    let li: String = if row % 11 == 9 {
        "[1, 2,\n null, 3]".to_string()
    } else if row % 11 == 5 {
        "[]".to_string()
    } else {
        format!("[{},{},null,{}]", row, row * 2, row * 3)
    };
    let wide: String = if row % 5 == 2 {
        "1000000000000000000000000000000".to_string()
    } else {
        format!("{:.3}", ((row * 29) % 500_000) as f64 / 16.0)
    };
    let ts_us: String = if row % 4 == 1 {
        "null".to_string()
    } else if row % 8 == 6 {
        "\"1969-12-31T23:59:59.999999\"".to_string()
    } else {
        format!("\"2024-0{}-1{}T0{}:17:45.123456\"", 1 + row % 9, 1 + row % 9, row % 10)
    };
    let ts_tz: String = if row % 4 == 3 {
        "null".to_string()
    } else {
        "\"2024-06-01T12:00:00.500+02:00\"".to_string()
    };
    format!(
        r#"{{"id":{row},"li":{li},"st":{st},"wide":{wide},"ts_us":{ts_us},"ts_tz":{ts_tz}}}"#
    )
}

/// Top-level JSON array of `nested_array_row` rows with whitespace between
/// elements.
fn generate_nested_array(path: &std::path::Path, rows: usize) -> std::io::Result<u64> {
    let file = std::fs::File::create(path)?;
    let mut out = BufWriter::with_capacity(1 << 20, file);
    out.write_all(b"[")?;
    for row in 0..rows {
        if row > 0 {
            out.write_all(b",")?;
        }
        out.write_all(nested_array_row(row).as_bytes())?;
    }
    out.write_all(b"]")?;
    out.flush()?;
    Ok(std::fs::metadata(path)?.len())
}

fn write_lines<F: Fn(usize) -> String>(
    path: &std::path::Path,
    rows: usize,
    gen: F,
) -> std::io::Result<u64> {
    let file = std::fs::File::create(path)?;
    let mut out = BufWriter::with_capacity(1 << 20, file);
    for row in 0..rows {
        out.write_all(gen(row).as_bytes())?;
        out.write_all(b"\n")?;
    }
    out.flush()?;
    Ok(std::fs::metadata(path)?.len())
}

/// Same rows as `mixed_row` but as one top-level JSON array with interior
/// whitespace between elements (array framing + whitespace tolerance).
fn generate_array_parity(path: &std::path::Path, rows: usize) -> std::io::Result<u64> {
    let file = std::fs::File::create(path)?;
    let mut out = BufWriter::with_capacity(1 << 20, file);
    out.write_all(b"[")?;
    for row in 0..rows {
        if row > 0 {
            out.write_all(b" , ")?;
        }
        out.write_all(mixed_row(row).as_bytes())?;
    }
    out.write_all(b"]")?;
    out.flush()?;
    Ok(std::fs::metadata(path)?.len())
}

// ---------------------------------------------------------------------------
// Fixture registry
// ---------------------------------------------------------------------------

const P_ROWS: usize = 2_000;
const ARRAY_ROWS: usize = 50_000;
const ARRAY_COLS: usize = 40;
const TEMPORAL_ROWS: usize = 100_000;
const LONGUTF8_ROWS: usize = 20_000;

fn fixtures_dir() -> std::path::PathBuf {
    std::env::var("O0J1_FIXTURES")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("/tmp/o0j1-fixtures"))
}

fn ensure_fixture(name: &str) -> (std::path::PathBuf, u64) {
    let dir = fixtures_dir();
    std::fs::create_dir_all(&dir).expect("create fixtures dir");
    let path = dir.join(name);
    let bytes = match name {
        "o0j1_array_40x50k.json" => {
            generate_array(&path, ARRAY_ROWS, ARRAY_COLS).expect("generate array fixture")
        }
        "o0j1_temporal_8x100k.ndjson" => {
            generate_temporal(&path, TEMPORAL_ROWS).expect("generate temporal fixture")
        }
        "o0j1_longutf8_3x20k.ndjson" => {
            generate_longutf8(&path, LONGUTF8_ROWS).expect("generate longutf8 fixture")
        }
        "o0j1_parity_mixed.ndjson" => {
            write_lines(&path, P_ROWS, mixed_row).expect("generate parity mixed")
        }
        "o0j1_parity_array.json" => {
            generate_array_parity(&path, P_ROWS).expect("generate parity array")
        }
        "o0j1_parity_nested.ndjson" => {
            write_lines(&path, P_ROWS, nested_row).expect("generate parity nested")
        }
        "o0j1_parity_nested_array.json" => {
            generate_nested_array(&path, P_ROWS).expect("generate parity nested array")
        }
        other => panic!("unknown fixture {other}"),
    };
    println!("O0J1FIXTURE {name} {bytes}");
    (path, bytes)
}

// ---------------------------------------------------------------------------
// Public-surface drain
// ---------------------------------------------------------------------------

fn connection(root: &std::path::Path) -> SourceConnection {
    SourceConnection::try_new(
        ConnectorKind::LocalFile,
        "fixtures",
        serde_json::json!({
            "allowedRoots": [root.to_str().expect("UTF-8 fixture path")],
            "schemaInference": { "maxRows": 1, "maxBytes": 8388608 }
        }),
        CredentialRef::new("cred://local/o0j1").expect("credential reference"),
    )
    .expect("connection")
}

fn registry() -> ConnectorRegistry {
    let mut registry = ConnectorRegistry::new();
    registry
        .register(Arc::new(LocalTabularConnector) as SourceConnectorRef)
        .expect("register connector");
    registry
}

fn open_stream(
    cell: &str,
    fixture: &std::path::Path,
    projection: Option<&[usize]>,
    override_kind: OverrideKind,
) -> stillflow_connectors::BatchStream {
    let registry = registry();
    let connection = connection(fixture.parent().unwrap());
    let name = fixture.file_name().unwrap().to_str().unwrap();
    let assets = futures::executor::block_on(registry.discover(
        &connection,
        DiscoverRequest {
            context: RequestContext::default(),
            parent_path: None,
        },
    ))
    .expect("discover");
    let asset = assets
        .iter()
        .find(|asset| asset.name == name)
        .unwrap_or_else(|| panic!("{name} discovered"))
        .clone();
    let metadata = futures::executor::block_on(registry.inspect(
        &connection,
        InspectRequest {
            context: RequestContext::default(),
            asset: asset.clone(),
        },
    ))
    .expect("inspect");

    let override_schema = match override_kind {
        OverrideKind::None => None,
        OverrideKind::Temporal => Some(pinned_override(&metadata.schema, TEMPORAL_TYPES)),
        OverrideKind::LongUtf8 => Some(pinned_override(&metadata.schema, LONGUTF8_TYPES)),
        OverrideKind::Nested => Some(pinned_override(&metadata.schema, NESTED_TYPES)),
    };
    let base_schema = override_schema.as_ref().unwrap_or(&metadata.schema);
    let projection_ids: Option<Vec<ColumnId>> = projection
        .map(|indices| indices.iter().map(|&i| base_schema.fields[i].id).collect());
    let mut request = ReadRequest::new(asset, BATCH);
    request.schema_override = override_schema;
    request.projection = projection_ids;
    let stream = futures::executor::block_on(registry.read_batches(&connection, request))
        .unwrap_or_else(|error| panic!("{cell}: open failed: {error}"));
    stream
}

enum OverrideKind {
    None,
    Temporal,
    LongUtf8,
    Nested,
}

type DeclaredTypes = &'static [(&'static str, fn() -> LogicalType, bool)];

fn ts(unit: TimeUnit) -> LogicalType {
    LogicalType::Timestamp { unit, timezone: None }
}

fn ts_tz(unit: TimeUnit) -> LogicalType {
    LogicalType::Timestamp {
        unit,
        timezone: Some("UTC".to_owned()),
    }
}

const TEMPORAL_TYPES: DeclaredTypes = &[
    ("id", || LogicalType::Int64, false),
    ("ts_ms", || ts(TimeUnit::Millisecond), true),
    ("ts_us", || ts(TimeUnit::Microsecond), true),
    ("ts_ns", || ts(TimeUnit::Nanosecond), true),
    ("ts_tz", || ts_tz(TimeUnit::Millisecond), true),
    ("d32", || LogicalType::Date32, true),
    ("label", || LogicalType::Utf8, false),
    ("val", || LogicalType::Float64, true),
];

fn nested_struct() -> LogicalType {
    LogicalType::Struct(vec![
        LogicalField::new(ColumnId::random(), "x", LogicalType::Int64, false).expect("x"),
        LogicalField::new(ColumnId::random(), "y", LogicalType::Utf8, true).expect("y"),
    ])
}

const LONGUTF8_TYPES: DeclaredTypes = &[
    ("id", || LogicalType::Int64, false),
    ("blob", || LogicalType::Utf8, false),
    ("nested", nested_struct, false),
];

fn nested_struct_nullable() -> LogicalType {
    LogicalType::Struct(vec![
        LogicalField::new(ColumnId::random(), "x", LogicalType::Int64, true).expect("x"),
        LogicalField::new(ColumnId::random(), "y", LogicalType::Utf8, true).expect("y"),
    ])
}

const NESTED_TYPES: DeclaredTypes = &[
    ("id", || LogicalType::Int64, false),
    ("li", || LogicalType::List(Box::new(LogicalType::Int64)), true),
    ("st", nested_struct_nullable, true),
    ("wide", || LogicalType::Float64, true),
    ("ts_us", || ts(TimeUnit::Microsecond), true),
    ("ts_tz", || ts_tz(TimeUnit::Millisecond), true),
];

fn pinned_override(schema: &LogicalSchema, types: DeclaredTypes) -> LogicalSchema {
    let fields = schema
        .fields
        .iter()
        .map(|source| {
            let (_, make_type, nullable) = types
                .iter()
                .find(|(name, _, _)| name == &source.name)
                .unwrap_or_else(|| panic!("declared type for {}", source.name));
            // Struct declarations embed freshly generated member ids per call;
            // member ids never surface in the output schema (names do).
            let data_type = make_type();
            LogicalField::new(source.id, source.name.clone(), data_type, *nullable)
                .expect("override field")
        })
        .collect();
    LogicalSchema::new(fields).expect("override schema")
}

/// Drains the stream to completion, returning (rows, envelopes, elapsed_ns,
/// terminal error if any).
fn drain(
    cell: &str,
    fixture: &std::path::Path,
    projection: Option<&[usize]>,
    override_kind: OverrideKind,
) -> (usize, usize, u128, Option<(ErrorCategory, String)>) {
    let started = Instant::now();
    let mut stream = open_stream(cell, fixture, projection, override_kind);
    let mut rows = 0_usize;
    let mut envelopes = 0_usize;
    let mut error = None;
    while let Some(item) = futures::executor::block_on(stream.next()) {
        match item {
            Ok(envelope) => {
                rows += envelope.row_count();
                envelopes += 1;
                let _ = envelope.payload().columns().len();
                let _ = envelope.payload().column(0).len();
            }
            Err(e) => {
                error = Some((e.category(), e.to_string()));
                break;
            }
        }
    }
    let elapsed = started.elapsed().as_nanos();
    drop(stream);
    (rows, envelopes, elapsed, error)
}

// ---------------------------------------------------------------------------
// Canonical digest of envelope payloads (value-equality witness)
// ---------------------------------------------------------------------------

struct Hasher(u64);

impl Hasher {
    fn new() -> Self {
        Self(0xcbf2_9ce4_8422_2325)
    }
    fn push(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    fn tag(&mut self, tag: &str, value: &str) {
        self.push(tag.as_bytes());
        self.push(b":");
        self.push(value.as_bytes());
        self.push(b";");
    }
}

fn digest_batch(batch: &RecordBatch, hasher: &mut Hasher) {
    let schema = batch.schema();
    for (field, column) in schema.fields().iter().zip(batch.columns()) {
        hasher.tag("col", field.name());
        hasher.tag("type", &type_tag(field.data_type()));
        hasher.tag("nullable", if field.is_nullable() { "1" } else { "0" });
        digest_array(field.data_type(), column.as_ref(), hasher);
    }
}

/// Metadata-free canonical Arrow type rendering: struct member column ids are
/// random per run, so they must never enter the digest.
fn type_tag(data_type: &DataType) -> String {
    match data_type {
        DataType::Null => "null".into(),
        DataType::Boolean => "bool".into(),
        DataType::Int8 => "i8".into(),
        DataType::Int16 => "i16".into(),
        DataType::Int32 => "i32".into(),
        DataType::Int64 => "i64".into(),
        DataType::UInt8 => "u8".into(),
        DataType::UInt16 => "u16".into(),
        DataType::UInt32 => "u32".into(),
        DataType::UInt64 => "u64".into(),
        DataType::Float32 => "f32".into(),
        DataType::Float64 => "f64".into(),
        DataType::Date32 => "date32".into(),
        DataType::Date64 => "date64".into(),
        DataType::Timestamp(unit, tz) => format!("ts({unit:?},{tz:?})"),
        DataType::Utf8 => "utf8".into(),
        DataType::LargeUtf8 => "largeutf8".into(),
        DataType::Binary => "bin".into(),
        DataType::LargeBinary => "largebin".into(),
        DataType::List(f) => format!("list({},{})", f.name(), type_tag(f.data_type())),
        DataType::LargeList(f) => {
            format!("largelist({},{})", f.name(), type_tag(f.data_type()))
        }
        DataType::Struct(fields) => {
            let inner = fields
                .iter()
                .map(|f| format!("{}:{}", f.name(), type_tag(f.data_type())))
                .collect::<Vec<_>>()
                .join(",");
            format!("struct({inner})")
        }
        other => panic!("type_tag: unsupported {other:?}"),
    }
}

fn digest_array(data_type: &DataType, array: &dyn Array, hasher: &mut Hasher) {
    macro_rules! bytes_primitives {
        ($ty:ty, $tag:expr) => {{
            let typed = array
                .as_any()
                .downcast_ref::<$ty>()
                .expect("payload bytes array type");
            for i in 0..array.len() {
                match typed.is_null(i) {
                    true => hasher.tag($tag, "~"),
                    false => {
                        let mut hex = String::with_capacity(typed.value(i).len() * 2);
                        for byte in typed.value(i) {
                            hex.push_str(&format!("{byte:02x}"));
                        }
                        hasher.tag($tag, &hex);
                    }
                }
            }
        }};
    }

    macro_rules! primitives {
        ($ty:ty, $tag:expr) => {{
            let typed = array
                .as_any()
                .downcast_ref::<$ty>()
                .expect("payload array type");
            for i in 0..array.len() {
                match typed.is_null(i) {
                    true => hasher.tag($tag, "~"),
                    false => hasher.tag($tag, &format!("{}", typed.value(i))),
                }
            }
        }};
    }
    match data_type {
        DataType::Null => {
            let typed = array.as_any().downcast_ref::<NullArray>().expect("null");
            for _ in 0..typed.len() {
                hasher.tag("null", "~");
            }
        }
        DataType::Boolean => primitives!(BooleanArray, "b"),
        DataType::Int8 => primitives!(Int8Array, "i8"),
        DataType::Int16 => primitives!(Int16Array, "i16"),
        DataType::Int32 => primitives!(Int32Array, "i32"),
        DataType::Int64 => primitives!(Int64Array, "i64"),
        DataType::UInt8 => primitives!(UInt8Array, "u8"),
        DataType::UInt16 => primitives!(UInt16Array, "u16"),
        DataType::UInt32 => primitives!(UInt32Array, "u32"),
        DataType::UInt64 => primitives!(UInt64Array, "u64"),
        DataType::Float32 => primitives!(Float32Array, "f32"),
        DataType::Float64 => primitives!(Float64Array, "f64"),
        DataType::Date32 => primitives!(Date32Array, "d32"),
        DataType::Date64 => primitives!(Date64Array, "d64"),
        DataType::Timestamp(unit, tz) => {
            let tag = format!("ts{unit:?}:{tz:?}");
            match unit {
                ArrowTimeUnit::Second => primitives!(TimestampSecondArray, &tag),
                ArrowTimeUnit::Millisecond => primitives!(TimestampMillisecondArray, &tag),
                ArrowTimeUnit::Microsecond => primitives!(TimestampMicrosecondArray, &tag),
                ArrowTimeUnit::Nanosecond => primitives!(TimestampNanosecondArray, &tag),
            }
        }
        DataType::Utf8 => primitives!(StringArray, "s"),
        DataType::LargeUtf8 => primitives!(LargeStringArray, "s"),
        DataType::Binary => bytes_primitives!(BinaryArray, "x"),
        DataType::LargeBinary => bytes_primitives!(LargeBinaryArray, "x"),
        DataType::List(_) => {
            let typed = array.as_any().downcast_ref::<ListArray>().expect("list");
            let values = typed.values();
            let value_type = match data_type {
                DataType::List(f) => f.data_type(),
                _ => unreachable!(),
            };
            for i in 0..typed.len() {
                if typed.is_null(i) {
                    hasher.tag("list", "~");
                } else {
                    let start = typed.value_offsets()[i] as usize;
                    let end = typed.value_offsets()[i + 1] as usize;
                    hasher.tag("list", &format!("len={}", end - start));
                    for j in start..end {
                        digest_array(value_type, &values.slice(j, 1), hasher);
                    }
                }
            }
        }
        DataType::Struct(fields) => {
            let typed = array
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("struct");
            for i in 0..typed.len() {
                if typed.is_null(i) {
                    hasher.tag("struct", "~");
                    continue;
                }
                for (field, column) in fields.iter().zip(typed.columns()) {
                    hasher.tag("sfld", field.name());
                    digest_array(field.data_type(), &column.slice(i, 1), hasher);
                }
            }
        }
        other => panic!("digest: unsupported payload type {other:?}"),
    }
}

fn parity(cell: &'static str, fixture: &std::path::Path, projection: Option<&[usize]>, override_kind: OverrideKind, expected_rows: usize) {
    let mut stream = open_stream(cell, fixture, projection, override_kind);
    let mut hasher = Hasher::new();
    let mut schema_text = String::new();
    let mut rows = 0_usize;
    let mut first = true;
    while let Some(item) = futures::executor::block_on(stream.next()) {
        match item {
            Ok(envelope) => {
                let batch = envelope.payload();
                rows += batch.num_rows();
                if first {
                    for field in batch.schema().fields() {
                        schema_text.push_str(&format!("{}:{:?};", field.name(), field.data_type()));
                    }
                    first = false;
                }
                digest_batch(batch, &mut hasher);
            }
            Err(e) => panic!("{cell}: unexpected stream error: {e}"),
        }
    }
    assert_eq!(rows, expected_rows, "{cell}: row count");
    println!(
        "O0J1DIGEST cell={cell} mode={MODE} rows={rows} digest={:016x} schema={schema_text}",
        hasher.0
    );
}

// ---------------------------------------------------------------------------
// Error cells
// ---------------------------------------------------------------------------

/// Writes an error fixture into the fixtures dir and returns its path.
fn error_fixture(name: &'static str) -> std::path::PathBuf {
    let dir = fixtures_dir();
    std::fs::create_dir_all(&dir).expect("create fixtures dir");
    match name {
        "e_wrong_array_element" => {
            let path = dir.join(format!("{name}.json"));
            std::fs::write(&path, "[{\"id\":1,\"label\":\"a\"}, \"not-an-object\"]")
                .expect("write error fixture");
            path
        }
        "e_truncated_array" => {
            let path = dir.join(format!("{name}.json"));
            std::fs::write(&path, "[{\"id\":1,\"label\":\"a\"}, {\"id\":2")
                .expect("write error fixture");
            path
        }
        _ => {
            let path = dir.join(format!("{name}.ndjson"));
            let content: &str = match name {
                "e_malformed_json" => "{\"id\":1,\"label\":\"a\"}\n{\"id\":2,\"label\":\n",
                "e_schema_drift" => "{\"id\":1,\"label\":\"a\"}\n{\"id\":\"two\",\"label\":\"b\"}\n",
                "e_malformed_temporal" => "{\"ts_ms\":\"2024-01-01T00:00:00.000\",\"label\":\"a\"}\n{\"ts_ms\":\"not-a-timestamp\",\"label\":\"b\"}\n",
                "e_unknown_field" => "{\"id\":1,\"label\":\"a\"}\n{\"id\":2,\"extra\":5,\"label\":\"b\"}\n",
                "e_duplicate_field" => "{\"id\":1,\"label\":\"a\"}\n{\"id\":2,\"id\":3,\"label\":\"b\"}\n",
                "e_missing_required" => "{\"id\":1,\"label\":\"a\"}\n{\"label\":\"b\"}\n",
                other => panic!("unknown error fixture {other}"),
            };
            std::fs::write(&path, content).expect("write error fixture");
            path
        }
    }
}

fn error_case(name: &'static str) {
    let fixture = error_fixture(name);
    let override_kind = if name == "e_malformed_temporal" {
        OverrideKind::Temporal
    } else {
        OverrideKind::None
    };
    let (rows, envelopes, elapsed, error) = drain(name, &fixture, None, override_kind);
    let (category, message) = error.unwrap_or_else(|| panic!("{name}: expected a stream error"));
    println!(
        "O0J1ERROR cell={name} mode={MODE} category={category:?} rows={rows} envelopes={envelopes} elapsed_ns={elapsed} message={message}"
    );
}

// ---------------------------------------------------------------------------
// Cells
// ---------------------------------------------------------------------------

const SPARSE_PROJECTION: [usize; 5] = [0, 10, 19, 29, 39];
const MIXED_PROJECTION: [usize; 3] = [0, 9, 14];

fn perf_cell(cell: &'static str) {
    let (fixture, expected_rows, projection, override_kind) = match cell {
        "array40x50k" => {
            let (fixture, _) = ensure_fixture("o0j1_array_40x50k.json");
            (fixture, ARRAY_ROWS, None, OverrideKind::None)
        }
        "temporal100k" => {
            let (fixture, _) = ensure_fixture("o0j1_temporal_8x100k.ndjson");
            (fixture, TEMPORAL_ROWS, None, OverrideKind::Temporal)
        }
        "longutf8" => {
            let (fixture, _) = ensure_fixture("o0j1_longutf8_3x20k.ndjson");
            (fixture, LONGUTF8_ROWS, None, OverrideKind::LongUtf8)
        }
        other => panic!("unknown perf cell {other}"),
    };
    let (rows, envelopes, elapsed, error) = drain(cell, &fixture, projection, override_kind);
    assert!(error.is_none(), "{cell}: unexpected error {error:?}");
    assert_eq!(rows, expected_rows, "{cell}: row count");
    let rss_end = rss_now();
    let vmhwm = vmhwm_now();
    println!(
        "O0J1SAMPLE cell={cell} mode={MODE} rows={rows} envelopes={envelopes} elapsed_ns={elapsed} rss_end_kb={rss_end} vmhwm_kb={vmhwm}"
    );
}

fn parity_cell(cell: &'static str) {
    match cell {
        "pmixed" => {
            let (fixture, _) = ensure_fixture("o0j1_parity_mixed.ndjson");
            parity(cell, &fixture, None, OverrideKind::None, P_ROWS);
        }
        "pmixed_narrow" => {
            let (fixture, _) = ensure_fixture("o0j1_parity_mixed.ndjson");
            parity(
                cell,
                &fixture,
                Some(&MIXED_PROJECTION),
                OverrideKind::None,
                P_ROWS,
            );
        }
        "parray" => {
            let (fixture, _) = ensure_fixture("o0j1_parity_array.json");
            parity(cell, &fixture, None, OverrideKind::None, P_ROWS);
        }
        "pnested" => {
            let (fixture, _) = ensure_fixture("o0j1_parity_nested.ndjson");
            parity(cell, &fixture, None, OverrideKind::Nested, P_ROWS);
        }
        "pnested_array" => {
            let (fixture, _) = ensure_fixture("o0j1_parity_nested_array.json");
            parity(cell, &fixture, None, OverrideKind::Nested, P_ROWS);
        }
        "pnarrow_sparse" => {
            let (fixture, _) = ensure_fixture("o0j1_array_40x50k.json");
            parity(
                cell,
                &fixture,
                Some(&SPARSE_PROJECTION),
                OverrideKind::None,
                ARRAY_ROWS,
            );
        }
        "ptemporal" => {
            let (fixture, _) = ensure_fixture("o0j1_temporal_8x100k.ndjson");
            parity(cell, &fixture, None, OverrideKind::Temporal, TEMPORAL_ROWS);
        }
        other => panic!("unknown parity cell {other}"),
    }
}

fn counters_cell(cell: &'static str) {
    let out_path = std::env::var("E24_IO_METRICS_OUT")
        .unwrap_or_else(|_| "/tmp/o0j1-counters.txt".to_string());
    std::env::set_var("E24_IO_METRICS_OUT", &out_path);
    let _ = std::fs::remove_file(&out_path);
    let (fixture, override_kind) = match cell {
        "array40x50k" => {
            let (fixture, _) = ensure_fixture("o0j1_array_40x50k.json");
            (fixture, OverrideKind::None)
        }
        "temporal100k" => {
            let (fixture, _) = ensure_fixture("o0j1_temporal_8x100k.ndjson");
            (fixture, OverrideKind::Temporal)
        }
        "longutf8" => {
            let (fixture, _) = ensure_fixture("o0j1_longutf8_3x20k.ndjson");
            (fixture, OverrideKind::LongUtf8)
        }
        other => panic!("unknown counters cell {other}"),
    };
    let started = Instant::now();
    let (rows, _, _, error) = drain(cell, &fixture, None, override_kind);
    assert!(error.is_none(), "{cell}: unexpected error {error:?}");
    let elapsed = started.elapsed().as_nanos();
    // The dump is written when the stream (and its PreparedReader) is dropped
    // inside `drain`, so the file now holds this run's cumulative counters.
    let text = std::fs::read_to_string(&out_path)
        .expect("io-metrics dump (build harness with --features metrics)");
    println!(
        "O0J1COUNTERS cell={cell} mode={MODE} rows={rows} elapsed_ns={elapsed} {}",
        text.trim().replace('\n', " ")
    );
}

fn rss_now() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").expect("read /proc/self/status");
    for line in status.lines() {
        if line.starts_with("VmRSS:") {
            return line
                .split_whitespace()
                .nth(1)
                .expect("VmRSS value")
                .parse()
                .expect("VmRSS number");
        }
    }
    panic!("VmRSS missing");
}

fn vmhwm_now() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").expect("read /proc/self/status");
    for line in status.lines() {
        if line.starts_with("VmHWM:") {
            return line
                .split_whitespace()
                .nth(1)
                .expect("VmHWM value")
                .parse()
                .expect("VmHWM number");
        }
    }
    panic!("VmHWM missing");
}

/// Debug: drain an arbitrary fixture and print the first `limit` canonical
/// values per column (per-mode diffing aid).
fn debug_cell(cell: &str) {
    let parts: Vec<&str> = cell.splitn(3, ',').collect();
    let fixture_path = std::path::PathBuf::from(parts[0]);
    let limit: usize = parts
        .get(2)
        .and_then(|n| n.parse().ok())
        .unwrap_or(8);
    let override_kind = match parts.get(1) {
        Some(&"temporal") => OverrideKind::Temporal,
        Some(&"longutf8") => OverrideKind::LongUtf8,
        Some(&"nested") => OverrideKind::Nested,
        _ => OverrideKind::None,
    };
    let mut stream = open_stream("debug", &fixture_path, None, override_kind);
    let mut printed = 0_usize;
    while printed < limit {
        let Some(item) = futures::executor::block_on(stream.next()) else {
            break;
        };
        let Ok(envelope) = item else {
            println!("DEBUG stream-error");
            break;
        };
        let batch = envelope.payload();
        let schema = batch.schema();
        for row in 0..batch.num_rows() {
            if printed >= limit {
                break;
            }
            let mut cells: Vec<String> = Vec::new();
            for (field, column) in schema.fields().iter().zip(batch.columns()) {
                cells.push(format!(
                    "{}={}",
                    field.name(),
                    debug_value(field.data_type(), column.as_ref(), row)
                ));
            }
            println!("DEBUG row={printed} {}", cells.join(" "));
            printed += 1;
        }
    }
}

fn debug_value(data_type: &DataType, array: &dyn Array, row: usize) -> String {
    macro_rules! prim {
        ($ty:ty) => {{
            let typed = array.as_any().downcast_ref::<$ty>().expect("array type");
            if typed.is_null(row) {
                "~".to_string()
            } else {
                format!("{:?}", typed.value(row))
            }
        }};
    }
    match data_type {
        DataType::Boolean => prim!(BooleanArray),
        DataType::Int64 => prim!(Int64Array),
        DataType::Float64 => prim!(Float64Array),
        DataType::Utf8 => prim!(StringArray),
        DataType::LargeUtf8 => prim!(LargeStringArray),
        DataType::Timestamp(_, _) => match data_type {
            DataType::Timestamp(ArrowTimeUnit::Second, _) => prim!(TimestampSecondArray),
            DataType::Timestamp(ArrowTimeUnit::Millisecond, _) => prim!(TimestampMillisecondArray),
            DataType::Timestamp(ArrowTimeUnit::Microsecond, _) => prim!(TimestampMicrosecondArray),
            DataType::Timestamp(ArrowTimeUnit::Nanosecond, _) => prim!(TimestampNanosecondArray),
            _ => unreachable!(),
        },
        DataType::Struct(fields) => {
            let typed = array
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("struct");
            if typed.is_null(row) {
                return "~".into();
            }
            let inner = fields
                .iter()
                .zip(typed.columns())
                .map(|(f, c)| format!("{}={}", f.name(), debug_value(f.data_type(), &c.slice(row, 1), 0)))
                .collect::<Vec<_>>()
                .join(",");
            format!("{{{inner}}}")
        }
        DataType::List(_) => {
            let typed = array.as_any().downcast_ref::<ListArray>().expect("list");
            if typed.is_null(row) {
                return "~".into();
            }
            let start = typed.value_offsets()[row] as usize;
            let end = typed.value_offsets()[row + 1] as usize;
            let values = typed.values();
            let value_type = match data_type {
                DataType::List(f) => f.data_type(),
                _ => unreachable!(),
            };
            let inner = (start..end)
                .map(|j| debug_value(value_type, &values.slice(j, 1), 0))
                .collect::<Vec<_>>()
                .join(",");
            format!("[{inner}]")
        }
        other => panic!("debug_value: unsupported {other:?}"),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 || args[1] != "run" {
        eprintln!("usage: o0j1-harness run <cell>");
        eprintln!("perf cells: array40x50k temporal100k longutf8");
        eprintln!("parity cells: pmixed pmixed_narrow parray pnested pnested_array pnarrow_sparse ptemporal");
        eprintln!(
            "error cells: e_malformed_json e_schema_drift e_malformed_temporal e_unknown_field e_duplicate_field e_missing_required e_wrong_array_element e_truncated_array"
        );
        eprintln!(
            "counter cells (build with --features metrics): counters_array40x50k counters_temporal100k counters_longutf8"
        );
        std::process::exit(2);
    }
    let cell: &'static str = Box::leak(args[2].clone().into_boxed_str());
    match cell {
        "array40x50k" | "temporal100k" | "longutf8" => perf_cell(cell),
        "pmixed" | "pmixed_narrow" | "parray" | "pnested" | "pnested_array"
        | "pnarrow_sparse" | "ptemporal" => {
            parity_cell(cell)
        }
        "e_malformed_json" | "e_schema_drift" | "e_malformed_temporal" | "e_unknown_field"
        | "e_duplicate_field" | "e_missing_required" | "e_wrong_array_element"
        | "e_truncated_array" => error_case(cell),
        "counters_array40x50k" | "counters_temporal100k" | "counters_longutf8" => {
            counters_cell(cell.strip_prefix("counters_").expect("counter cell"))
        }
        _ if cell.starts_with("debug:") => debug_cell(&cell["debug:".len()..]),
        other => {
            eprintln!("unknown cell {other}");
            std::process::exit(2);
        }
    }
}
```

### A.3 `run_measurements.sh` (flock wrapper: 1 warm-up + N timed samples)

```bash
#!/usr/bin/env bash
# O0-J1 measurement driver (issue #283). External to the repo; not committed.
#
# Concurrency policy: every timed run is serialized on the shared machine via
# `flock /tmp/stillflow-o0-measure.lock`. Each sample is a fresh process, so
# VmHWM is that sample's true peak. One untimed warm-up per cell/mode precedes
# the timed samples (page cache + lazy statics).
#
# Usage: run_measurements.sh <bin> <cell-kind> <cell> <mode-label> <runs>
set -eu

BIN="$1"
KIND="$2"     # run | e24 | parity | error | counters
CELL="$3"
MODE="$4"     # off | on
RUNS="$5"
OUT_DIR="${O0J1_RESULTS:-/home/owl/o0j1-harness/results}"
FIXDIR=/tmp/o0j1-fixtures
LOCK=/tmp/stillflow-o0-measure.lock

mkdir -p "$OUT_DIR"
export O0J1_FIXTURES="$FIXDIR"
export E24_EVIDENCE_FIXTURES=/tmp/e24-158-fixtures

# Warm-up (untimed, unlocked is not allowed for runs that could take seconds;
# keep it inside the lock too so it never overlaps a sibling's timed run).
case "$KIND" in
  run)
    flock "$LOCK" -c "O0J1_FIXTURES=$FIXDIR '$BIN' run '$CELL'" >/dev/null
    ;;
  parity)
    flock "$LOCK" -c "O0J1_FIXTURES=$FIXDIR '$BIN' run '$CELL'" >/dev/null
    ;;
  error)
    flock "$LOCK" -c "O0J1_FIXTURES=$FIXDIR '$BIN' run '$CELL'" >/dev/null
    ;;
  counters)
    : # no warm-up; counters run once
    ;;
  e24)
    FEAT=""
    [ "$MODE" = on ] && FEAT="--features json-direct-projected-writer"
    flock "$LOCK" -c "cd /home/owl/stillflow-o0j1/backend && CARGO_TARGET_DIR=/home/owl/.cargo-o0-target cargo test -p stillflow-connector-local-tabular --release $FEAT --test e24_json_a2_prod_evidence -- '$CELL' --ignored --nocapture" >/dev/null
    ;;
esac

for i in $(seq 1 "$RUNS"); do
  case "$KIND" in
    run|parity|error|counters)
      flock "$LOCK" -c "O0J1_FIXTURES=$FIXDIR '$BIN' run '$CELL'" | grep -E '^O0J1' | sed "s/^/run=$i /"
      ;;
    e24)
      FEAT=""
      [ "$MODE" = on ] && FEAT="--features json-direct-projected-writer"
      flock "$LOCK" -c "cd /home/owl/stillflow-o0j1/backend && CARGO_TARGET_DIR=/home/owl/.cargo-o0-target cargo test -p stillflow-connector-local-tabular --release $FEAT --test e24_json_a2_prod_evidence -- '$CELL' --ignored --nocapture" 2>/dev/null | grep -E '^E24SAMPLE'
      ;;
  esac
done >> "$OUT_DIR/${MODE}_${KIND}_${CELL}.txt"
echo "wrote $OUT_DIR/${MODE}_${KIND}_${CELL}.txt"
```

### A.4 `stats.py` (P50/P95/spread/CV over the sample lines)

```python
#!/usr/bin/env python3
"""P50/P95/spread stats for O0-J1 sample files (issue #283 evidence).

Reads sample lines (O0J1SAMPLE / E24SAMPLE) from result files and prints a
table: cell, mode, runs, P50, P95, min, max, spread%, CV%.

Usage: stats.py <file>...
"""
import math
import re
import sys


def percentile(sorted_values: list[float], p: float) -> float:
    """Nearest-rank percentile on an ascending list."""
    if not sorted_values:
        raise ValueError("empty")
    k = max(0, min(len(sorted_values) - 1, math.ceil(p / 100 * len(sorted_values)) - 1))
    return sorted_values[k]


def main(paths: list[str]) -> None:
    rows = []
    for path in paths:
        values = []
        cell = mode = ""
        rows_seen = set()
        for line in open(path):
            m = re.search(r"(?:O0J1SAMPLE|E24SAMPLE)\s+cell=(\S+)\s+mode=(\S+)", line)
            if m:
                cell, mode = m.group(1), m.group(2)
            m = re.search(r"elapsed_ns=(\d+)", line)
            if m:
                values.append(int(m.group(1)))
            m = re.search(r"rows=(\d+)", line)
            if m:
                rows_seen.add(int(m.group(1)))
            m = re.search(r"vmhwm_kb=(\d+)", line)
            if m:
                rows.append((path, "vmhwm", int(m.group(1))))
        if not values:
            print(f"{path}: no samples")
            continue
        s = sorted(values)
        p50 = percentile(s, 50)
        p95 = percentile(s, 95)
        mn, mx = s[0], s[-1]
        mean = sum(s) / len(s)
        stdev = (sum((v - mean) ** 2 for v in s) / len(s)) ** 0.5
        cv = stdev / mean * 100 if mean else 0.0
        spread = (mx - mn) / p50 * 100 if p50 else 0.0
        print(
            f"{path}: cell={cell} mode={mode} n={len(s)} "
            f"p50={p50/1e6:.3f}ms p95={p95/1e6:.3f}ms min={mn/1e6:.3f}ms "
            f"max={mx/1e6:.3f}ms spread={spread:.1f}% cv={cv:.1f}% "
            f"rows={sorted(rows_seen)}"
        )


if __name__ == "__main__":
    main(sys.argv[1:])
```
