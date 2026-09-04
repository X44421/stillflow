# H1 Golden E2E evidence matrix

Issue: #244 — H1 Golden E2E deterministic backend matrix
Baseline: `main@a3e0b556c2bb7e51822063d80dbf732e7a048192`
Scope: integration/evidence only; no runtime semantic changes.

The H1 gate composes the existing E5 Job/Run/Event runtime, Q profile/quality
surfaces, X export runtime/API, connector boundaries, and durable storage. Each
row names the exact test(s) that provide evidence. The exact-head CI run for
this change must be recorded here after the H1 PR is pushed.

| Gate | Exact test evidence | CI evidence |
| --- | --- | --- |
| H1-01 canonical full pipeline | `csv_source_to_materialize_snapshot`; `csv_full_lifecycle_verify_profile_export`; `h1_api_export_format_matrix_recomputes_file_and_set_digests` | PR exact-head `backend-tests-msrv` |
| H1-02 required input formats | `h1_api_input_format_matrix_covers_tsv_and_json`; `all_formats_inspect_project_preview_and_stream_in_stable_batches`; `ndjson_complete_supported_lifecycle`; `parquet_complete_supported_lifecycle`; `workbook_applicable_lifecycle`; `s3_compatible_bounded_range_aware_lifecycle` | PR exact-head `backend-tests-msrv` |
| H1-03 required Export formats | `h1_api_export_format_matrix_recomputes_file_and_set_digests`; `csv_golden_header_null_empty_quote_delimiter_lf_cr`; `tsv_golden_same_edges_with_tab_delimiter`; `jsonl_golden_field_order_escaping_numeric_timestamp`; `parquet_golden_canonical_schema_snappy_metadata` | PR exact-head `backend-tests-msrv` |
| H1-04 Profile / Findings / Quality | `csv_full_lifecycle_verify_profile_export`; `csv_events_artifacts_version_and_secret_bounds`; `q_a1_profile_history_drift_api_uses_one_e5_lifecycle` | PR exact-head `backend-tests-msrv` |
| H1-05 Plan / Preview / Run | `csv_source_to_materialize_snapshot`; `plan_save_version_cas_and_digest_are_stable_through_api`; `csv_full_lifecycle_verify_profile_export` | PR exact-head `backend-tests-msrv` |
| H1-06 Verification | `csv_full_lifecycle_verify_profile_export`; `export_manifest_records_exact_provenance_and_recomputable_digests`; `post_inference_drift_is_typed_and_batch_partition_is_invariant` | PR exact-head `backend-tests-msrv` |
| H1-07 Job / Run / Event lifecycle | `csv_events_artifacts_version_and_secret_bounds`; `thousand_plus_event_replay_is_paginated_and_bounded` | PR exact-head `backend-tests-msrv` |
| H1-08 Export / X-G1 sub-gate | `csv_full_lifecycle_verify_profile_export`; `h1_api_export_format_matrix_recomputes_file_and_set_digests`; `deterministic_repeated_export_byte_equality` | PR exact-head `backend-tests-msrv` |
| H1-09 bounded API reads/download | `csv_full_lifecycle_verify_profile_export`; `csv_events_artifacts_version_and_secret_bounds`; `thousand_plus_event_replay_is_paginated_and_bounded` | PR exact-head `backend-tests-msrv` |
| H1-10 edge cases | `empty_sources_zero_field_rows_and_nullability_are_preserved`; `preview_reports_row_and_byte_truncation_independently`; `long_string_row_stays_under_the_batch_byte_bound`; `rejects_malformed_inputs_unknown_projection_and_unsupported_operations`; `post_inference_drift_is_typed_and_batch_partition_is_invariant`; `corrupt_source_materialize_fails_closed_without_partial_artifact` | PR exact-head `backend-tests-msrv` |
| H1-11 determinism | `post_inference_drift_is_typed_and_batch_partition_is_invariant`; `deterministic_repeated_export_byte_equality`; `h1_api_export_format_matrix_recomputes_file_and_set_digests` | PR exact-head `backend-tests-msrv` |
| H1-12 restart / cancellation / replay | `restart_preserves_state_events_lineage_and_digests`; `restart_reconciles_queued_and_running_jobs_without_partial_artifacts`; `cancellation_race_reaches_running_job_without_second_state_machine`; `queued_cancel_is_durable_and_terminal_cancel_is_idempotent`; `duplicate_submit_replays_and_conflicts_deterministically` | PR exact-head `backend-tests-msrv` |

## Local verification command

```bash
cd backend
cargo test -p stillflow-api --test e5_g1_runtime_e2e --all-features --no-fail-fast
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace -- --skip total_output_cap_is_accepted_at_eight_gib_and_enforced_above
```

## Boundary

H1 records evidence for existing semantics. A failing gate that requires a
runtime behavior change becomes a separate `[H1-BNN]` blocker Issue and is not
fixed in this H1 change. X-G1 is absorbed as H1-08; no independent X-G1
implementation task is created. SEC, AUD, AUT, OPS, H2, and H3 remain deferred
roadmap nodes until H1 and Phase 1 closeout are complete.
