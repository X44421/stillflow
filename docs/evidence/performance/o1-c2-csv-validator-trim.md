# O1-C2 — CSV validator: provably-redundant re-verification removal evidence

- Version: 1
- Date: 2026-09-06
- Issue: #299 (`[O1-C2] Remove provably redundant CSV validation`)
- Exact measured head: `3ac61697a19c12ed0d94fb9687604f2f5182665f` (implementation
  tip, three commits). The branch was then rebased onto `main@e36f099` (which
  brought in PR #306, the svc-a1 service-entry layer — no file overlap with the
  connector or engine); the three commits carried over verbatim. The measured
  binary therefore does not contain #306, and no measured code path
  (connector-local-tabular CSV read) is touched by it.
- Baseline: `main@9a78c6b` (the O1-C1 delivery head), measured in the same
  worktree, same target directory, same harness, interleaved arm-by-arm.
- Raw records: [`o1-c2-records.jsonl`](./o1-c2-records.jsonl) — every harness
  record of both campaigns below, tagged with campaign/round/arm.

## 1. What was removed, and the accept-set proof (O0-C1 §11.4 oracle)

Two commits, each removing only re-derivation the strict decode provably owns:

1. **Digit-only integer fast path** (`int_fast_accept`): an all-ASCII-digit
   cell skips the `str::parse` re-attempt. Out-of-range digit strings fail in
   the decoder first (pinned: `128`/Int8, `9223372036854775808`/Int64,
   39-digit overflow — decoder-normalized message, no row), so a digit-only
   cell that reaches the validator is always parseable and the re-parse can
   only re-derive acceptance. Every other spelling keeps the original
   predicate verbatim.
2. **Short digit/dot-only float fast path** (`numeric_fast_accept`, ≤ 32
   bytes): magnitude is bounded far below `f32::MAX`, so the cell is finite
   whenever the decoder accepted it. Overflow-to-infinity spellings (measured:
   `1e309`, 309-digit integers, 400-digit integers — all decoder-ACCEPTED and
   validator-rejected, `at row N`) are longer or exponent-bearing and keep the
   original `parse + is_finite` predicate. Degenerate dot forms the scan
   admits (`"."`, multi-dot) are decoder-first rejects (pinned).
3. **Byte-record validation with lazy per-cell UTF-8 views** (`read_byte_record`
   + `text_value`): the former `StringRecord` path validated UTF-8 and copied
   the whole record per row; the decoder already owns UTF-8 validity — invalid
   UTF-8 fails at inspection inside the bounded inference prefix (pinned:
   `InvalidData`, "text source is not valid UTF-8") and at the decoder beyond
   it (pinned: normalized message, 1 499 136 rows emitted before failure). The
   lazy `str::from_utf8` fallback is therefore unreachable defense-in-depth.

**Probe-proven load-bearing surfaces that were NOT touched** (all produce the
granular `delimited value does not match the established schema at row 1`):
leading-whitespace numerics (`" 1"` — trailing/both-side whitespace and tabs
are decoder-first, only the leading-only class reaches the validator); every
Rust non-finite spelling variant (`inf`, `+inf`, `-inf`, `Inf`, `INFINITY`,
`iNf`, `infinity`, `-infinity`, `NaN`, `nan`, `+NaN`, `NAN`); case-variant
booleans (`TRUE`/`True`/`FALSE` — `"1"`/`"0"`/`"yes"` are decoder-first);
slash-form dates (`2024/01/03` — the decoder accepts them, the chrono pattern
rejects); naive-timestamp `Z` suffix and no-seconds forms; required empty
fields (two-column form). The width check stays as defense-in-depth behind the
`flexible(false)` reader and the decoder (both fire first — pinned), per the
O1-C1 suite note ("preserve or consciously change").

Oracle: the new `csv_validator_accept_set_corpus.rs` (10 tests) pins the
per-spelling terminating surface for every class above; the O1-C1 suite
(`csv_validation_reference.rs`, 24 tests) passes **unchanged**.

## 2. Machine and measurement discipline

Same machine as the O0 round (i3-12100F 6 vCPU, WSL2, `--release`, shared
target directory for both arms). Every timed invocation ran inside
`flock /tmp/stillflow-o0-measure.lock`. Each invocation is one process running
the O0-C1 harness as-is (1 untimed warm-up + 7 timed reps, 5 for the wide
case, harness-internal P50/P95 + counter P50s across reps; fixture digests
match the O0-C1 published SHAs).

**Protocol note — why interleaved:** a first sequential campaign (all head
cases, then all baseline cases) showed uniform ~+9% inflation of every stage
across the head arm (validate/wall ratios identical between arms), i.e.
machine-state drift, not code effect. Those records are included tagged
`sequential-1-superseded` (the baseline records carry a head-stamp typo, a
trailing `x`, kept as-is for disclosure) and support no claim. The published
campaign interleaves arms: 7 rounds × 3 cases × 2 arms, one process per
invocation, all inside one flock hold.

## 3. Results (interleaved campaign; median across 7 rounds of the
harness-internal P50s)

| Case | Fixture | Base wall | Head wall | Δ wall | Base validate | Head validate | Δ validate |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| full-csv-wide-mixed-128c-100k (numeric-heavy; primary affected) | 108 393 184 B | 831.0 ms | 820.0 ms | **−1.3%** | 284.3 ms | 244.2 ms | **−14.1%** |
| full-csv-anchor-10c-100k (all-Utf8) | 65 000 115 B | 271.0 ms | 266.0 ms | −1.8% | 88.1 ms | 85.1 ms | −3.4% |
| full-csv-narrow-fixed-8c-100k (mixed) | 10 200 024 B | 69.0 ms | 70.0 ms | +1.4% | 20.9 ms | 20.0 ms | −4.1% |

Round spreads of wall P50 (base vs head): wide 749–885 vs 702–885;
anchor 254–290 vs 255–281; narrow 62–79 vs 62–79. Decode-stage deltas
(−2.6% to +6.5% across cases) are untouched code — they bound the noise floor.

Lockstep intactness held in every record of both campaigns:
`csv_rows_decoded = csv_rows_validated = 100 000` and
`validator_read_bytes` = exact file bytes (100.00%).

## 4. Acceptance-criterion status — discrepancy flagged for adjudication

Issue #299 acceptance: *"Ingest gain on the affected scenarios exceeds the
#293 §7 noise threshold at the final head."* The §7 thresholds are **ingest
wall** thresholds (≥ 30% P50 for ≥ 1 s ingestion, ≥ 50% for sub-second). The
measured ingest-wall deltas (−1.3% / −1.8% / +1.4%) are inside the noise
floor, so **the criterion is not met at wall level**.

This was predictable from the O0-C1 arithmetic and is now measured: the whole
validation pass is 26.5–36.0% of ingest wall (O0-C1 §6); the O1-C1 suite gate
pins the lockstep structure, which protects the text re-parse half; the
per-cell re-verification targeted here is 0–41% of that pass (O0-C1 §6.2).
Even a perfect zeroing of per-cell re-verification cannot exceed ~13% of
ingest wall. This substep measured **−14.1% of the validation stage** on the
primary affected scenario — a real, above-stage-noise reduction (decode-stage
noise is ±2.6–6.5% on the same fixture) — but the wall-level criterion is
unreachable for ANY change admitted by this task's boundaries.

Decision requested from the maintainer: (a) accept the stage-level metric for
O1-C2 substeps and treat the wall criterion as applying to the §11.2-class
follow-up (metadata reuse), which requires relaxing the O1-C1 gate itself and
is out of this task's boundaries; or (b) hold this PR.

## 5. Correctness acceptance

- O1-C1 suite unchanged and green (24/24), the accept-set corpus green
  (10/10), the O1-J1 runtime dual-arm oracle green (26/26), connector crate
  full suite green (23 unit + 11 local_tabular + 1 memory_bound), engine suite
  green (237/237), `cargo fmt --all -- --check` clean,
  `cargo clippy --workspace --all-targets` zero warnings.
- One-PR rollback point: revert the three commits (or, per commit: the fast
  paths and the byte-record switch are independently revertible; the corpus
  test pins both).

## 6. Reproduction

`O0_C1_MODE=generate|measure` per case, `O0_C1_FIXTURE_ROOT`,
`--release -p stillflow-connector-local-tabular --features io-metrics`, exact
commands in the O0-C1 note §5; arms = the two binaries built from
`3ac6169…` and `9a78c6b…` respectively. All records in
[`o1-c2-records.jsonl`](./o1-c2-records.jsonl).
