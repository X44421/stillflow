# Phase 1 deterministic backend MVP closeout evidence

Issue: #246 — P1-CLOSE
Main at audit: `497180991f7edc3cea63b6d3277b09b10cdb71a7`
Scope: review/docs-only; no Phase 1 runtime changes.

## Gate disposition

| Gate | Evidence | Disposition |
| --- | --- | --- |
| H1 Golden E2E | Issue [#244](https://github.com/X44421/stillflow/issues/244) closed; PR [#245](https://github.com/X44421/stillflow/pull/245) merged; exact-head receipt [#5537145811](https://github.com/X44421/stillflow/pull/245#issuecomment-5537145811) | PASS |
| H1 exact-head CI | [run 33847243823](https://github.com/X44421/stillflow/actions/runs/33847243823), 6/6 required jobs passed | PASS |
| H1 independent evidence | Clean detached checkout at `171158ca30f04305745dcaff062dba00af4f3503`; target E2E test 19/19 passed | PASS |
| X-G1 Export sub-gate | H1-08 in `docs/evidence/h1/golden-e2e.md`; CSV/TSV/JSONL/Parquet API Export, Manifest, bounded bytes, per-file and set digest evidence | PASS |
| Post-merge main | [run 33848535037](https://github.com/X44421/stillflow/actions/runs/33848535037), head `497180991f7edc3cea63b6d3277b09b10cdb71a7`, 6/6 passed | PASS |

The H1 implementation head is a direct parent of the merge commit. The merge
commit parents are the H1 base `a3e0b556c2bb7e51822063d80dbf732e7a048192` and
the accepted implementation head `171158ca30f04305745dcaff062dba00af4f3503`.

## Real Phase 1 semantic operations

The current main contains executable implementations and the H1 real-API
matrix exercises their composed path:

- Source discovery, inspect, bounded preview: `ApiService::discover_source_assets`, `inspect_source_asset`, and `preview_source_asset`, delegated to the connector registry.
- Import/materialization and Snapshot publication: `JobRuntime` and `Engine::materialize`, with committed Snapshot output asserted by the H1 CSV/TSV/JSON/NDJSON/Parquet/Workbook/S3 coverage.
- Verification and rejected/finding semantics: `Engine::materialize_verification` plus the existing validation, deduplication, bounded-report, corruption, cancellation, and deadline evidence.
- Profile, Quality, and Drift: the existing typed engine/storage paths and `q_a1_profile_history_drift_api_uses_one_e5_lifecycle`, with API readback and provenance assertions.
- Export and Manifest: `ApiService::submit_export` constructs the typed `JobOperation::Export` and delegates to the existing JobRuntime; H1 recomputes published bytes, file SHA-256, and ordered set digest.

## Placeholder and authority audit

The production scan found no `todo!`, `unimplemented!`, `TODO`, or `FIXME`
implementation markers. The only `placeholder` matches are a Polars FFI
comment describing a physical-Null buffer and the function
`validate_evidence_placeholder`, which performs structural FindingEvidence
validation; neither is a product placeholder endpoint or execution path.

The frontend `src/` contains presentation/sample data and a result-preview
toast but no materialize, JobOperation, executor, or Export execution path.
API Export submits through the existing typed JobRuntime, and Engine remains the
execution authority. No second queue, state machine, digest algorithm, or
canonical cleaning implementation was introduced.

## Legacy and roadmap reconciliation

- Legacy Issue [#11](https://github.com/X44421/stillflow/issues/11) is already CLOSED; its historical acceptance is retained, and H1/#246 provide the current deterministic Phase 1 closeout evidence.
- Epic [#81](https://github.com/X44421/stillflow/issues/81) is the roadmap/dependency authority. Its next-node status must be synchronized by the P1-CLOSE completion receipt to mark Phase 1 closed and make P23-C0 the only next mainline node.
- No H1 blocker was found, so no H1-BNN Issue was created.

## Completion boundary

After this closeout, Phase 1 is frozen as:

> Phase 1 deterministic backend MVP closed.

P23-C0 is the next mainline node. SEC, AUD, AUT, OPS, H2, and H3 remain
deferred until their dependencies are explicitly frozen and dispatched. The
P1 Registry claim/lock is released at technical completion.
