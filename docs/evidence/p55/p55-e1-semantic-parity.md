# P55-E1 — Polars 0.46 → 0.55.2 semantic parity over the acceptance corpus

- Issue: #399 (`[P55] Polars 0.46 → 0.55.2 分阶段迁移`)
- Nature: **verification only**. This document changes no production file; it
  records the semantic-parity evidence for the dependency uplift delivered by
  PR #404 (P55-D1/A1/A2) on top of PR #403 (P55-T0).
- Baseline head (polars `0.46`, Rust `1.98.0`): `40ec6d4` — PR #403 head
- Measured head (polars `0.55.2`, Rust `1.98.0`): `aa900f5` — PR #404 head
- Measured on: 2026-09-22
- Branches: `agent/issue-399-p55-t0-toolchain-uplift`,
  `agent/issue-399-p55-d1-polars-uplift`

## 1. Environment

| Item | Value |
| --- | --- |
| Host OS | Debian GNU/Linux 13 (trixie) on WSL2, kernel `6.18.33.2-microsoft-standard-WSL2 x86_64` |
| CPU / RAM | 3 cores / 6 hardware threads (`nproc` = 6) / ~11 GiB |
| Rust toolchain | `rustc 1.98.0 (88d9e12ae 2026-08-18)`, `cargo 1.98.0`, pinned by `rust-toolchain.toml` (P55-T0) |
| Cargo concurrency | `jobs = 2` (`~/.cargo/config.toml`), dedicated `CARGO_TARGET_DIR` per migration node |
| Command | `cd backend && cargo test --workspace [--no-fail-fast] -- --skip total_output_cap_is_accepted_at_eight_gib_and_enforced_above` |

## 2. Method

The plan's acceptance rule is used verbatim:

```text
same input + same StillFlow plan = same StillFlow observable result
```

The corpus is **value-level**, not smoke-level: it asserts decoded values
(including exact per-unit timestamp epochs), canonical body digests, schema
fingerprints, ColumnId mapping, row counts and batch boundaries, error
categories and stable messages, cancellation and memory-bound behaviour. The
`nx-v1` fixtures under `stillflow-plan/tests/fixtures` are frozen captures of
pre-change behaviour and are compared byte-for-byte, so a passing suite means
the recorded outputs still match — not merely that nothing panicked.

No golden fixture was regenerated for this migration. The single corpus change
is one **added** fail-closed test (§4.1).

## 3. Result

| | 0.46 (`40ec6d4`) | 0.55.2 (`aa900f5`) |
| --- | --- | --- |
| Test suites | 53 | 53 |
| Passed | 841 | 841 |
| Failed | 0 | 1 (§4.2, environment flake) |
| Ignored | 11 | 11 |
| New tests | — | +1 (`naive_timestamps_still_fail_closed_on_malformed_text`) |

Per-area outcome on `0.55.2` (all `ok` unless noted):

| Area | Suites |
| --- | --- |
| CSV / TSV decode + dual-reader validation | `csv_validation_reference` (25), `csv_validator_accept_set_corpus` (10), `read_baseline`, `o0_c1_csv_dup_work`, `local_tabular` (11), `memory_bound` (1) |
| JSON / NDJSON | `direct_projected_writer` (26), `e24_json_a2_prod_evidence`, `nx_a1_strict_decode` (4) |
| Parquet + connector surface | `stillflow-connector-local-tabular` lib (23), `object_store_connector` (1), `workbook_connector` (7) |
| Expressions / node rules | `nx_n1_field_shaping` (16), `nx_n2_text_cleaning` (12), `nx_n3_temporal_parsing` (15), `nx_n4_concat` (6), `nx_n4_conditional` (5), `nx_n4_contains` (7), `nx_n4_substring` (10), `nx_n2_composite` (5 + 4) |
| Runtime / determinism | `nx_s1_differential` (5), `nx_s1_semantics` (6), `nx_s0_sort` (18), `nx_b1_resources` (2), engine lib suites (116 + 249 + 12 + 8), `stillflow-plan` lib (133) |
| Storage / service / API | `stillflow-storage` lib (27), `stillflow-service` lib (9) + `http_entry_e2e` (§4.2) + `e5_g1_runtime_e2e` (19), `stillflow-api` lib (8) + `aud_a1_api` (3) + `e5_e1_event_stream` (5) + `ops_o1_observability` (2) |

## 4. Differences found, classified

### 4.1 Decoded result difference — **Polars bug fix** (adjudicated)

- Input: `t\n2024-01-31T12:34:56\n2024-01-31 12:34:56.5\n` with a naive
  `Timestamp` schema.
- `0.46`: the strict decoder rejected the column (`source data is malformed or
  incompatible with the established schema`, `SchemaDrift`) — the O1-C1
  reference suite pinned that rejection as fail-closed.
- `0.55.2`: the column decodes; measured values are exact for every declared
  unit (`Second`: `1706704496` / `1706704496`, `Millisecond`:
  `1706704496000` / `1706704496500`, and the same ×10³ / ×10⁶ for
  microsecond / nanosecond).
- Why it is a parser fix and not a contract break:
  `csv_validator_accept_set_corpus::naive_timestamp_accept_set_is_pinned`
  already pinned each spelling **alone** as accepted, so 0.46 was rejecting a
  format-inference artifact of mixing `T` and space spellings in one column.
- Adjudication in the corpus: the mixed-spelling case now asserts the exact
  epochs (`naive_timestamps_decode_with_the_declared_unit_over_csv`), and a new
  `naive_timestamps_still_fail_closed_on_malformed_text` keeps the fail-closed
  surface pinned: impossible dates, garbage text and trailing-space text still
  fail in the decoder; zoned text on a naive column still fails in the
  validator with the one-based row.

### 4.2 Execution difference — **environment requirement** (neutralised in A2)

- `0.55` lowers a `LazyFrame` through
  `polars_plan::dsl_to_ir::fetch_metadata` →
  `polars_async::RuntimeManager::block_in_place_on` →
  `tokio::task::block_in_place`, which panics on a single-threaded Tokio
  runtime. That made 47 node tests panic under `#[tokio::test]`
  (`current_thread`) whereas 0.46 executed them.
- Production was never affected (`stillflow-server` builds
  `Builder::new_multi_thread()`), and no golden value changed.
- Adjudication: the engine adapter `polars_adapter::blocking` runs the blocking
  lowering on a scoped plain thread when the ambient runtime is
  single-threaded; on multi-threaded runtimes (production) the call stays
  inline. A probe confirmed both halves: direct `collect()` inside a
  current-thread runtime panics, the same call on a scoped thread succeeds.

### 4.3 Isolated failure that is not a migration difference

`stillflow-service --test http_entry_e2e ::
t_ng_g1_compile_preview_plan_version_restart_and_snapshot` failed once inside
the fully parallel `--no-fail-fast` workspace run with:

```text
service starts: Storage(Busy("managed root is already owned"))
```

The test passes alone, and the whole `http_entry_e2e` binary passes 12/12 when
run on its own. The failing crate is untouched by this migration; the failure
is a cross-binary isolation race over a shared managed root, and it did not
reproduce in CI for either PR (run `35619170124` for #403, `35727187505` for
#404 — both completed `success`, including the `Backend tests (Rust 1.98.0)`
job).

## 5. Coverage against the plan's freeze list

| Planned freeze area | Where it is pinned | 0.55.2 result |
| --- | --- | --- |
| CSV: malformed rows, quoting, header, projection, schema drift | `csv_validation_reference`, `csv_validator_accept_set_corpus`, `read_baseline`, `o0_c1_csv_dup_work` | pass |
| JSON: top-level array, NDJSON, duplicate keys, nested, wide numbers, direct-projected parity | `direct_projected_writer`, `e24_json_a2_prod_evidence` | pass |
| Expressions: trim, normalize, cast, temporal, contains, concat, conditional, substring | `nx_n1`, `nx_n2`, `nx_n3`, `nx_n4_*` | pass |
| Runtime: preview, batch boundaries, memory bound, cancellation, schema fingerprint, materialization | engine lib suites, `nx_s1_*`, `nx_b1_resources`, `memory_bound` | pass |
| Determinism: canonical digests, ColumnId, row order | `nx-v1` fixtures, `nx_s1_differential`, engine lib suites | pass |

## 6. Residual risk / not covered here

- **No side-by-side byte differential in one process.** Both Polars versions
  cannot coexist in one build of a crate, so parity is established by running
  the same value-level corpus at two exact heads rather than by diffing two
  live engines. A dump-and-diff harness (canonical digests per case, both
  heads, machine-readable records) would strengthen this and is the natural
  follow-up if the migration's risk review asks for it.
- **Performance parity is not claimed** and is deliberately deferred to P55-B1:
  the compiler, the dependency graph and the physical executor all changed at
  once, so the old O0/O1 numbers are not a valid comparison base.
- The 8 GiB physical export boundary is skipped by the routine command (as in
  CI) and remains owned by the dedicated slow workflow.
- `_csv_read_internal` is an underscore-prefixed Polars module; its use is
  confined to `polars_adapter/csv_batch.rs`.
