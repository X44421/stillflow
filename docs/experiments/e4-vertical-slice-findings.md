# E4 experimental vertical slice findings

Status: experimental probe only. **Do not merge this branch. Do not
cherry-pick the implementation wholesale onto `main`.** Do not treat
`2958548ccc26832ee2978a7ae8744ebc677a42a0` as evidence that Issue #54 /
PR #57 is satisfied.

Feed the design conclusions in this file into PR #57 (E4-C0-R4). Formal E4
runtime must still start from an approved contract on merged `main`, then
reconstruct only the pieces that contract names.

Base: `origin/main` @ `85502cbebb1fab461fe42d30fe019ad20613aa7c` (E2 / PR #49).
Probe commit: `2958548ccc26832ee2978a7ae8744ebc677a42a0` on
`experiment/e4-vertical-slice`.
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

## Verdict

The loop runs. Crate arrows and the dual entry (`materialize` vs
`materialize_verification`) are the right direction. The commit still has
seven high-priority defects and is **not** a mergeable E4, **not** a complete
contract proof, and **not** a reusable storage publication.

Keep this branch as an architecture probe. Revise #57 from the conclusions
below. Rewrite storage publication in formal E4; do not lift
`stillflow-storage` bundle/journal/recovery/digest code from this branch.

## Probe answers

| Question | Result |
| --- | --- |
| Can E2 executors be reused for Validate / Deduplicate? | Yes, with a second entry point. `materialize` still returns `UnsupportedRule`. `materialize_verification` reuses preflight, chunking, Polars, FFI, `CanonicalRebatcher`, and the run gate. Validate / Dedup are applied after Scan output, not inside `apply_rule`. |
| Can accepted and rejected rows both be bounded? | Functionally yes on tiny smoke data. Each stream is packed by `CanonicalRebatcher` under `MAX_BATCH_BYTES`. The live-payload tracker still models only three columnar payloads, so the rejected sidecar is not actually enforced as a fourth live payload. |
| Is `VerificationBundle` implementable? | The membership + four-artifact shape is implementable as a storage publication. The probe's publication, reload, digest, and recovery code is **known incorrect** (P1-1, P1-5, P1-7). Formal E4 must not reuse it. |
| Can Snapshot be Export input? | Yes. `export_snapshot_to_csv` reads the committed accepted snapshot and does not re-read the source. Accepted partitions must keep the E2 `{sequence}-{digest}.parquet` final name. |
| Do crate ownership arrows cycle? | No. Types live in `stillflow-core`, publication in `stillflow-storage`, orchestration in `stillflow-engine`. `stillflow-plan` is unchanged as a physical-free IR. Engine must not take adapter crates even as dev-dependencies (`t19`). |
| Does a fourth large payload break the memory law? | The smoke tests do not prove the law. Scan-output clones used as rejected payloads are a fourth `MAX_BATCH_BYTES`-class live buffer that E2 §12.1 does not list. `MemoryTracker` still counts envelope + Polars/incoming + one remainder. Two remainders (accepted + rejected) overwrite each other in the tracker. Findings are a further unbounded live buffer (P1-3). |

## Blockers — known incorrect, not "uncovered"

These are data-correctness, identity, bound, or recovery defects in
`2958548`. They are **blockers** for any merge and for treating the probe as
#57 acceptance evidence. A later findings sentence that says "not covered"
must not be read as softening these.

### P1-1 — Accepted provenance mutates on reload

Commit returns the real accepted `ArtifactProvenance`. `load_bundle()` rebuilds
it from `accepted_manifest_provenance_stub()`, which zeros `version_digest`,
plan digest, and plan fingerprint; copies `created_at` into started/committed
times; and sets `engine_build` to `"experimental"`.

Same artifact, different provenance, after process restart or store reopen.
R4 V24/V25/V26 require committed provenance to round-trip.

Formal E4 must persist accepted provenance, or derive it losslessly from the
already-persisted bundle provenance plus snapshot stats. Add a test that
commit result equals close-store-then-reload result.

Locations: `bundle.rs` commit path ~L988–L998; reload path ~L1311–L1377.

### P1-2 — `FilterRows` then Validate/Dedup writes the wrong rejected row and ordinal

Ordinary rules only update `working`. They do not filter `scan_df` or
`ordinals`. Later Validate/Dedup index the pre-filter sidecar.

Example: drop row 0 with `FilterRows`, then fail row 1. RejectedRows can store
row 0's Scan payload and ordinal.

R4 §5.1.4 already requires this combination: later `Filter`/`FilterRows` drop
rows **without renumbering**; rejected payload is still the logical Scan
output row. The probe bug is a **known data-correctness defect**, not a
missing test. Smoke plans simply avoid the combination.

Experiment-only mitigation: preflight-reject `FilterRows`/`Filter` after Scan
on this branch. Formal E4 must keep Scan sidecar and ordinals in lockstep
(stable gaps), not delete FilterRows from the contract.

Locations: `verification.rs` rule dispatch ~L478–L518; Validate routing
~L536–L586.

### P1-3 — Findings stay in memory for the whole run, with an O(n²) path

`RoutingState` accumulates every validation and duplicate finding in `Vec`.
`REPORT_PACK_ROWS` / `REPORT_PACK_BYTES` are unused. Each validation insert
scans the existing Vec to count findings for the same ordinal (O(n²) when
many rows fail). The writer then builds one Arrow batch and copies provenance
strings per row.

This can exhaust memory or the deadline before envelope/storage ceilings
fire. It is a seventh (and unbounded) live payload relative to R4 §12.1.

Formal E4 must pack findings as they are produced, or at least enforce a
strict per-run row/byte cap before append. Per-row finding caps must be
O(1) counters, not a scan of all findings.

Locations: `verification.rs` `RoutingState` ~L46–L57; `push_validation_finding`
~L749–L778; report batch construction ~L864–L895.

### P1-4 — `version_digest` is caller-declared

The engine recomputes and checks `canonical_plan_digest`. For logical input
it only checks that the asset UUID matches. Tests inject `[0x11; 32]`. Any
digest can enter reports and durable provenance.

R4 §8.1.1 already requires the engine to recompute
`LogicalInputRef.version_digest` from the bound asset/schema and reject a
mismatch. Do not weaken that clause because the probe skipped it.

Locations: `verification.rs` ~L109–L115 and ~L991–L998;
`e4_engine_csv.rs` ~L100–L104.

### P1-5 — Crash recovery is not bundle-aware

Publication journal is keyed by accepted snapshot ID. Staging is named by
bundle ID. Reports, rejected rows, and dedup artifacts use their own artifact
IDs. Existing `SnapshotStore::recover` only walks snapshot IDs.

Crash after installing files and before the SQLite commit can leave report
and rejected final directories. Dedup uses `create_new` for `.lock`/`.sqlite`;
a leftover pair makes the same `run_id` permanently `AlreadyExists`.

R4 §10.4 already names Prepared/Staged/Installing/Committing against
**bundle** staging and member artifacts. The E2 snapshot-keyed journal is
not sufficient. Formal E4 must not reuse this publication implementation.

Locations: `bundle.rs` begin ~L172–L216 and install ~L420–L470;
`store.rs` recover ~L259–L301; `dedup.rs` ~L160–L195.

### P1-6 — Report `ColumnId` values collide across sections

All four report schemas allocate IDs as `REPORT_COLUMN_BASE + field_index`.
Different semantics, and sometimes different types, share the same
`ColumnId`. That breaks schema identity and cross-version reads.

R4 §8.7 already froze distinct constants (`0x...0021`–`0x...005B`). Runtime
must never generate report IDs. Copy the constants; do not keep positional
allocation.

Locations: `report.rs` `REPORT_COLUMN_BASE` ~L34; `report_schema` ~L469–L483.

### P1-7 — Digest and manifest integrity are incomplete

Accepted `content_digest` hashes the snapshot UUID under a domain prefix, not
`accepted_snapshot_manifest_digest` from R4 §8.1.1. Bundle digest omits child
artifact/manifest/content digests. Load reads stored section/manifest digest
and stats; it does not recompute and cross-check.

The earlier sentence "digests hash durable Parquet bytes" is **false** for
accepted and bundle provenance. Partition files may be hashed; accepted and
bundle content digests are not a Parquet preimage and are not the R4
canonical preimage either.

Formal E4 needs `LogicalSchema::canonical_bytes()`, Arrow IPC batch bodies,
and reload-time recomputation (V25). Do not substitute Parquet-file SHA-256
for those formulas.

Locations: `bundle.rs` ~L988–L998, ~L1068–L1083, ~L1380–L1467.

### Adjacent bound bug — variable-width dedup keys copy first

Utf8/Binary key encoding copies the whole value, then checks the 64 KiB cap.
A large cell can allocate past the operator-state budget before
`BoundExceeded`. Check remaining capacity (or `try_reserve` the exact next
size) **before** `extend`. Matches R4 reserve-before-allocate and V20.

Location: `canonical.rs` ~L116–L130.

## Contract deviations the experiment took

These are shortcuts, not approved exceptions:

1. Digests are not R4 §8.1.1 canonical preimages. Accepted/bundle digests are
   weaker than even "hash the Parquet file" (P1-7).
2. Caller always injects `rejected_rows_artifact_id: Some(_)`. Membership is
   `None` when zero rows were rejected; the caller cannot know
   `terminal_rejection_count` before the run. This membership rule is worth
   keeping; the "always Some in the request" injection is a probe API.
3. Findings are buffered in `Vec`, not packed at `REPORT_PACK_ROWS` /
   report-byte remainders (P1-3).
4. `CanonicalRebatcher` still flushes at `MAX_BATCH_BYTES` / `batch_size`, not
   a 2 MiB report pack.
5. Unix `0700` / `0600` modes are best-effort; they are no-ops on Windows.
6. Storage schema version 2 is experimental and must not land from this branch.
7. `FilterRows` inside `ApplyRules` after Scan output desynchronizes the
   rejected sidecar. That is known incorrect (P1-2), not an allowed gap.
8. `source_row_ordinal` is assigned after Scan projection and Scan.predicate.
   This matches R4 §5.1 and should be kept.
9. Accepted snapshot partitions must use the E2 `{sequence}-{digest}.parquet`
   final name. Sequence-only names made `read_batches` fail integrity and broke
   Snapshot → CSV Export until that was aligned. Keep this E2 alignment.

## Smoke coverage (this branch)

Covered, as a **loop probe**, not as V01–V30:

- happy scripted path in `stillflow-engine`
- a real CSV file through `stillflow-connector-local-tabular` (engine stays
  adapter-free; the composition test lives in the connector crate)
- invalid rows split accepted / rejected
- exact-dedup keep-first across two batches
- empty stream → 0-row accepted snapshot, reports present, no rejected artifact
- all rows rejected → empty accepted snapshot, rejected artifact present
- zero rejections → no empty RejectedRows snapshot
- cancel **before write** → no visible bundle
- fail during write (`max_partitions = 1`, `batch_size = 1`, two accepted rows)
  → no visible bundle
- same identities repeated → `AlreadyExists`; different identities, same input
  → identical CSV
- sentinel cell value absent from `EngineError` Display / Debug / sanitized
  summary
- `materialize()` still rejects Validate / Dedup

Not covered, and the gaps matter:

- real CSV rejection or dedup (the file test is three unique valid rows)
- successful bundle after `SnapshotStore` close and reopen (P1-1 would fail)
- cancel at write/install/commit points; residue of staging, final dirs, and
  dedup temp files; same-id retry after fail (P1-5)
- exact byte and memory ceiling tests (E2 three-payload tracker still in use)
- partition / batch-size invariance
- crash recovery and fault injection
- idempotency, concurrency, and cancellation races
- `FilterRows` then Validate/Dedup (P1-2)
- MSRV / stable / Clippy / workspace CI as a merge gate
- Issue #54 V01–V30 mapped onto named test functions

Local checks on the probe host: `cargo fmt`, Clippy, engine tests, and the
connector CSV test passed. Workspace `cargo test` still fails pre-existing
Windows-only storage lock / workbook-root tests that this branch did not
change. GitHub had no CI status on `2958548` at review time.

## Reuse vs rewrite

Reuse as **design**, then reimplement from merged `main` after #57 is
approved. Do not cherry-pick these modules as-is.

Worth reconstructing:

- Dual entry: `materialize` stays E2; `materialize_verification` is the E4
  path.
- Crate arrows: core identities, storage publication, engine orchestration,
  plan remains physical-free, engine adapter-free.
- Ordinal assignment after Scan projection + `Scan.predicate`.
- Validate/Dedup after Scan output, not a second lowering language.
- Zero-rejection membership: no empty RejectedRows artifact.
- Accepted snapshot as CSV export input; E2 partition file names.
- Keep-first exact dedup **direction** via a storage-owned SQLite index, not
  an in-memory `HashMap`.
- Injected identities/timestamps; engine-recomputed plan digest.

Must rewrite (do not copy this branch):

- All of `stillflow-storage` bundle publication, journal, install, recover,
  load-stub provenance, and digest formulas (P1-1, P1-5, P1-7).
- `report.rs` positional `ColumnId` allocation (P1-6).
- Whole-run findings `Vec` and O(n²) per-row count (P1-3).
- Caller-trusted `version_digest` (P1-4).
- Scan sidecar / ordinal lockstep for FilterRows (P1-2).
- Utf8/Binary key encode-then-check (adjacent bound bug).
- `MemoryTracker` still on the E2 three-payload model.

## Recommendation for #57 (E4-C0-R4)

Keep the dual-track split. The slice shows the loop can run. It also shows
that publication identity, recovery, digests, report schema identity, and
the six-payload memory law fail if implemented as this probe did.

Do **not** weaken R4 because the experiment took shortcuts. In particular:

| R4 clause | Probe result | Contract action |
| --- | --- | --- |
| Dual entry; `materialize` still `UnsupportedRule` (V23) | Holds | Keep |
| Crate ownership §7.1 | Holds | Keep |
| Ordinals after Scan projection/predicate §5.1 | Holds | Keep |
| FilterRows drops without renumbering; rejected payload is Scan output §5.1.4 / §5.3 | Probe is **wrong**; contract is right | Keep. Add an explicit FilterRows → Validate Error fixture (Scan payload + original ordinal, not a shifted index) |
| Engine recomputes `version_digest` and rejects mismatch §8.1.1 | Probe skipped it | Keep; do not trust the caller |
| Fixed report `ColumnId` constants §8.7 | Probe generates colliding IDs | Keep; runtime must copy constants |
| `REPORT_PACK_*` and six live payloads §12.1 / V29 | Probe unbounded Vec + E2 tracker | Keep; treat an all-run findings `Vec` as a stop condition |
| Journal-before-staging; Installing cleans **bundle** members §10.4 / V30 | Probe reuses snapshot-keyed E2 recover | Keep; E2 `snapshot_id` journal is not enough |
| Dedup lock-first; leftover files `AlreadyExists`; recovery under maintenance gate §9 / V12 | Probe `create_new` can poison the run forever | Keep; crash leftover must not be a permanent same-run poison without recovery |
| Canonical digest formulas + reload recomputation §8.1.1 / V25 | Probe hashes UUID / omits children / stub-reloads accepted provenance | Keep. Require persist-or-lossless-derive of accepted provenance; commit == close-store reload |
| Reserve-before-allocate for variable-width keys V20 | Probe copies Utf8/Binary then checks | Spell "check remaining capacity before `extend`" if R5 is opened |
| Zero-rejection rule §10.2 / V11 | Holds on smoke | Keep |

Formal E4 should start from an approved contract on merged `main`, then
reconstruct only the pieces the contract names. This branch remains a
read-only probe.
