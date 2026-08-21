# E4 experimental vertical slice findings

Status: experimental probe only. Do not merge this branch. Do not cherry-pick
the implementation wholesale onto `main`. Feed these conclusions into Issue
#57 / PR #57, then implement the approved contract from merged `main`.

Base: `origin/main` @ `85502cbebb1fab461fe42d30fe019ad20613aa7c` (E2 / PR #49).
Issue #53 and the unapproved #57 contract were not used as merge bases.

Closed loop under test:

```text
CSV / scripted batches
→ LogicalPlan (Scan → Validate → Deduplicate → Materialize)
→ bounded E2 transform path
→ accepted / rejected rows
→ VerificationBundle + DatasetSnapshot
→ CSV export from the accepted snapshot
```

## Probe answers

| Question | Result |
| --- | --- |
| Can E2 executors be reused for Validate / Deduplicate? | Yes, with a second entry point. `materialize` still returns `UnsupportedRule`. `materialize_verification` reuses preflight, chunking, Polars, FFI, `CanonicalRebatcher`, and the run gate. Validate / Dedup are applied after Scan output, not inside `apply_rule`. |
| Can accepted and rejected rows both be bounded? | Functionally yes on tiny smoke data. Each stream is packed by `CanonicalRebatcher` under `MAX_BATCH_BYTES`. The live-payload tracker still models only three columnar payloads, so the rejected sidecar is not actually enforced as a fourth live payload. |
| Is `VerificationBundle` implementable? | The membership + four-artifact shape is implementable as a storage publication. The probe uses Parquet-file SHA-256 plus domain prefixes, not the unapproved §8.1.1 envelope preimage (`LogicalSchema` has no `canonical_bytes()`). Crash-recovery states (Prepared / Staged / Installing / Committing) are not fully implemented; abort is Drop + SQLite `abort_publication`. |
| Can Snapshot be Export input? | Yes. `export_snapshot_to_csv` reads the committed accepted snapshot and does not re-read the source. |
| Do crate ownership arrows cycle? | No. Types live in `stillflow-core`, publication in `stillflow-storage`, orchestration in `stillflow-engine`. `stillflow-plan` is unchanged as a physical-free IR. |
| Does a fourth large payload break the memory law? | The smoke tests do not prove the law. Scan-output clones used as rejected payloads are a fourth `MAX_BATCH_BYTES`-class live buffer that §12.1 of the E2 contract does not list. `MemoryTracker` still counts envelope + Polars/incoming + one remainder. Two remainders (accepted + rejected) overwrite each other in the tracker. |

## Contract deviations the experiment took

1. Digests hash durable Parquet bytes, not canonical `BatchEnvelope` preimages.
2. Caller always injects `rejected_rows_artifact_id: Some(_)`. Membership is
   `None` when zero rows were rejected; the caller cannot know
   `terminal_rejection_count` before the run.
3. Findings are buffered in `Vec`, not packed at `REPORT_PACK_ROWS` /
   report-byte remainders.
4. `CanonicalRebatcher` still flushes at `MAX_BATCH_BYTES` / `batch_size`, not
   a 2 MiB report pack.
5. Unix `0700` / `0600` modes are best-effort; they are no-ops on Windows.
6. Storage schema version 2 is experimental and must not land from this branch.
7. `FilterRows` inside `ApplyRules` after Scan output does not keep the rejected
   sidecar in lockstep. Smoke plans do not use that combination.
8. `source_row_ordinal` is assigned after Scan projection and Scan.predicate.
9. Accepted snapshot partitions must use the E2 `{sequence}-{digest}.parquet`
   final name. Sequence-only names made `read_batches` fail integrity and broke
   Snapshot → CSV Export until that was aligned.

## Smoke coverage (this branch)

Covered:

- happy scripted path in `stillflow-engine`
- a real CSV file through `stillflow-connector-local-tabular` (engine stays
  adapter-free; the composition test lives in the connector crate)
- invalid rows split accepted / rejected
- exact-dedup keep-first across two batches
- empty stream → 0-row accepted snapshot, reports present, no rejected artifact
- all rows rejected → empty accepted snapshot, rejected artifact present
- zero rejections → no empty RejectedRows snapshot
- cancel before write → no visible bundle
- fail during write (`max_partitions = 1`, `batch_size = 1`, two accepted rows)
  → no visible bundle
- same identities repeated → `AlreadyExists`; different identities, same input
  → identical CSV
- sentinel cell value absent from `EngineError` Display / Debug / sanitized
  summary
- `materialize()` still rejects Validate / Dedup

Not covered, and still required before any merge to `main`:

- exact byte and memory ceiling tests
- partition / batch-size invariance
- crash recovery and fault injection
- idempotency, concurrency, and cancellation races
- MSRV / stable / Clippy / workspace CI as a merge gate
- Issue #54 V01–V30 mapped onto named test functions

## Recommendation for #57

Keep the dual-track split. The slice shows the loop can run. It also shows the
dangerous gaps: a fourth live payload, digest preimage, and publication state
machine. Formal E4 should start from an approved contract on merged `main`,
then reconstruct only the pieces the contract names.
