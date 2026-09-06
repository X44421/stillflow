# O1-R1 — Combined performance regression on final merged main

- Version: 1
- Date: 2026-09-06
- Issue: #301 (`[O1-R1] Combined performance regression on final merged main`)
- Measured heads:
  - **final arm**: `main@05cc91d` — the complete O1 round merged (O1-J1 #302,
    O1-P1 #304, O1-C1 #305, O1-C2 #308, O1-S1 #309; plus #306, which touches
    the service-entry layer only).
  - **baseline arm**: `main@0af8f38` — the O1 round's dispatch base (final
    main of the O0 round), the same baseline the per-PR O1 evidence notes
    used.
- Raw records: [`o1-r1-records.jsonl`](./o1-r1-records.jsonl) — all 144
  harness records (3 interleaved rounds × 22 scenarios × 2 arms, plus the
  JSON-direct supplementary rounds), tagged with case/round/arm/knob.

## 1. Harness and protocol

The O0-B1 harness (`tests/o0_b1_baseline.rs`, #293) runs unmodified: one case
per process, `#[ignore]`d, `io-metrics`-gated, harness-internal 1 warm-up +
30/7/5 timed reps (per the case's published rep count), P50/P95/min/max,
CPU-tick and VmHWM attribution, per-run row-stability and SHA-256 digest
witnesses. The early-release scenario reuses the O0-C1 harness
(`bounded-earlydrop-csv-anchor-10c-3batches`, 3 × 4096-row envelopes then
stream drop).

**One measurement-only harness commit** (disclosed): an `O0_B1_JSON_DIRECT`
env knob on the harness's connection builder that sets the O1-J1
`jsonDirectProjectedWriter` connection key when explicitly requested. It is
structurally inert unless the env var is set; every default-path run in both
arms left it unset, so both arms' default-path harnesses behave identically.

Protocol: **case-level interleaved A/B, 3 rounds** — for each round, every
scenario runs base-then-final as one process per invocation inside
`flock /tmp/stillflow-o0-measure.lock`; per-case numbers are medians across
the 3 rounds of the harness-internal wall P50s. Fixture digests match the
#293 published SHA-256s. Interleaving (rather than one campaign per arm) is
the load-bearing design choice: sibling-load episodes during this campaign
(round 3 shows matching outliers in BOTH arms on three cases) would otherwise
have manufactured phantom regressions or improvements.

Machine: same as the O0/O1 rounds (i3-12100F 6 vCPU, WSL2, `--release`,
page-cache warm) — no hardware delta vs #293 to disclose; machine *condition*
deltas (sibling conversations) are mitigated by interleaving and disclosed
per-round in the records.

## 2. Results — default paths (the regression gate)

Median wall P50 across 3 rounds, ms. "Envelope" = the larger arm's maximal
per-round P95/P50 spread. The regression gate applied: **a scenario is
flagged only if its median delta exceeds its own envelope.**

| Scenario | base | final | Δ | envelope | verdict |
| --- | ---: | ---: | ---: | ---: | --- |
| ingest-csv-anchor-10c-100k | 275.0 | 270.0 | −1.8% | 41.5% | within |
| ingest-csv-anchor-100c-100k | 2670.0 | 2560.0 | −4.1% | 23.9% | within |
| ingest-csv-anchor-10c-1m | 2341.0 | 2417.0 | +3.2% | 20.2% | within |
| ingest-ndjson-anchor-10c-100k | 818.0 | 786.0 | −3.9% | 27.5% | within |
| ingest-ndjson-anchor-100c-100k | 11850.0 | 10948.0 | −7.6% | 31.5% | within |
| ingest-json-array-anchor-10c-100k | 1232.0 | 1106.0 | −10.2% | 50.8% | within |
| ingest-parquet-anchor-10c-100k | 299.0 | 298.0 | −0.3% | 34.8% | within |
| ingest-parquet-anchor-100c-100k | 3808.0 | 3318.0 | −12.9% | 83.2% | within |
| ingest-csv-narrow-fixed-8c-100k | 63.0 | 53.0 | **−15.9%** | 43.2% | within |
| ingest-csv-wide-mixed-128c-100k | 643.0 | 674.0 | +4.8% | 29.7% | within |
| ingest-csv-longutf8-8c-100k | 345.0 | 340.0 | −1.4% | 62.4% | within |
| ingest-ndjson-timestamps-10c-100k | 297.0 | 303.0 | +2.0% | 89.8% | within |
| ingest-csv-malformed-10c-60k | 1.0 | 1.0 | +0.0% | — | identical early-failure path |
| ingest-ndjson-malformed-10c-30k | 80.0 | 76.0 | −5.0% | 65.0% | within |
| engine-narrow-simple-8c-100k | 149.0 | 157.0 | +5.4% | 54.9% | within |
| engine-wide-mixed-128c-100k | 2040.0 | 2015.0 | −1.2% | 17.0% | within |
| engine-rule-heavy-8c-100k | 403.0 | 419.0 | +4.0% | 212.9% | within |
| engine-expression-heavy-8c-100k | 614.0 | 608.0 | −1.0% | 46.3% | within |
| engine-parquet-100c-100k | 6929.0 | 7370.0 | +6.4% | 82.5% | within |
| engine-ndjson-timestamps-override-10c-100k | 553.0 | 528.0 | −4.5% | 26.4% | within |
| engine-narrow-write-8c-1m | 2032.0 | 1916.0 | **−5.7%** | 22.0% | within |
| bounded-earlydrop-csv-anchor-10c-3batches | 67.0 | 69.0 | +3.0% | 125.4% | within |

**Verdict: no scenario regresses beyond its measured envelope; the O1 round
composes.** The positive deltas (engine-parquet +6.4%, narrow-simple +5.4%,
wide-mixed ingest +4.8%, 1M anchor +3.2%, earlydrop +3.0%) are
direction-consistent across rounds but each sits inside a wide, outlier-driven
envelope, with no monotone trend across rounds 1→3; the records disclose the
per-round values. Combined improvements are visible where the O1 work
targets them: CSV narrow ingest (O1-C2 fast paths), the 1M-row write E2E,
ndjson-100c/json-array/parquet-100c ingest, and engine ndjson-timestamps.

Behavior witnesses (every case, both arms): row counts stable across all
reps; digest witnesses recorded; both error-path scenarios produce byte
identical error witnesses across arms and rounds
(`InvalidData`/`source data is malformed…` and
`InvalidData`/`JSON row does not match the established schema at row 15001`).

## 3. JSON-direct supplementary (opt-in path characterization, final head)

`O0_B1_JSON_DIRECT=1` vs the default path at the final head (3 rounds each;
the direct path did not exist as a runtime knob at the baseline head, so this
compares enablement mechanisms at the final head only):

| Scenario | default | direct | Δ |
| --- | ---: | ---: | ---: |
| ingest-ndjson-anchor-10c-100k | 786.0 | 577.0 | **−26.6%** |
| ingest-ndjson-anchor-100c-100k | 10948.0 | 9423.0 | **−13.9%** |
| ingest-json-array-anchor-10c-100k | 1106.0 | 2599.0 | **+135.0%** |
| ingest-ndjson-timestamps-10c-100k | 303.0 | 538.0 | **+77.6%** |

The production default remains off, so these are **not** regressions — they
are a characterization of the opt-in path that sharpens #302's documented
low-benefit boundaries: the direct projected assembler wins on wide NDJSON
projections (consistent with #302's measured −43.2% on its wide-table main
scenario) and loses badly on JSON-array input and timestamp-heavy NDJSON.
Recommended follow-up (documentation-only): add these two shapes to #302's
boundary list in its evidence note, and keep the runtime switch default-off
(it already is). No action is required for safety.

## 4. Acceptance mapping (issue #301)

- [x] Every scenario within the agreed regression threshold against a fresh
      same-conditions baseline run for this note (gate: scenario's own
      measured envelope; no scenario exceeds it).
- [x] Machine condition deltas vs #293 disclosed (same hardware; sibling-load
      episodes disclosed via per-round records; interleaving neutralizes
      them).
- [x] Combined effect measured on the merged result; per-PR gains were never
      summed arithmetically.
- [x] Results published as an evidence note with the exact measured heads and
      raw records, following the O0 provenance conventions.

## 5. Reproduction

`O0_B1_MODE=generate|measure`, `O0_B1_FIXTURE_ROOT`, `--release -p
stillflow-connector-local-tabular --features io-metrics`, case list in
`tests/o0_b1_baseline.rs`; the early-drop case via the O0-C1 harness
(`O0_C1_CASE=bounded-earlydrop-csv-anchor-10c-3batches`). Arms: binaries built
from `05cc91d` and `0af8f38` respectively. Every record in
[`o1-r1-records.jsonl`](./o1-r1-records.jsonl).
