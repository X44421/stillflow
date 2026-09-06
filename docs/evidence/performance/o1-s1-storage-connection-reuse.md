# O1-S1 — SQLite connection reuse within a single operation

- Version: 1
- Date: 2026-09-06
- Issue: #300 (`[O1-S1] SQLite connection reuse within a single operation`)
- Exact measured head: `3168254` (implementation commit `perf(storage): reuse
  one SQLite connection per snapshot publication`; the only later commit on
  the branch is this evidence note).
- Base: `main@e36f099` (includes the O1 connector/engine merges and PR #306;
  PR #306 touches no storage file — verified by diff). The task text's
  recorded dispatch base (`main@0af8f38`, #291) was superseded by the O1
  merges — the storage crate is untouched by them.
- Evidence base: O0-S1 (#291,
  [`o0-s1-storage-costs.md`](./o0-s1-storage-costs.md)) measured the SQLite
  connection lifecycle (open + frozen three-PRAGMA configuration) per
  operation touchpoint.

## 1. Operation selection (exactly one, ranked by #291's cost data)

**Selected: the snapshot publication lifecycle** (`begin_snapshot` →
`append`* → `commit`, with the drop path `abort`). Ranked from #291's
attribution:

- A successful publication touched the database at **two** independent
  connection lifecycles (journal insert + manifest commit), an aborted one at
  **three** (plus the best-effort journal cleanup) — each lifecycle paying one
  open + three PRAGMAs. #291 attributes ~0.4–1.1 ms of open+PRAGMA
  configuration per extra connection.
- Publications are on every snapshot write path (high frequency), unlike the
  rejected candidates: `tombstone_snapshot` / `tombstone_export` (each one
  connection per call, low frequency, destructive-path risk), `recover`
  (already one connection per pass), `load_manifest` (read-side, one
  connection per read — a different, higher-risk reuse surface), and the
  control-plane autocommit ops (one connection per statement is their
  documented shape).
- Streaming digest was explicitly out of scope this round (#291 measured its
  share as low) and was not touched.

## 2. Change

`SnapshotWriter` owns exactly one configured `rusqlite::Connection` for the
whole operation. `begin_snapshot` opens it once; `insert_publication`,
`commit_manifest`, and the best-effort `abort_publication` (including the
`Drop` path) take the operation-owned connection instead of opening their own.
`Connection` is `Send`, not `Sync`, matching the exclusive `&mut self` append
API; ownership ends with the writer (commit success, commit failure, or drop).

Counting witness (deterministic, in
`tests/o0s1_metrics_neutrality.rs`, run with and without the
`storage-metrics` feature):

| Quantity (per publication lifecycle) | Before | After |
| --- | ---: | ---: |
| `ConnectionOpen` events | 2 (3 on abort path) | **1** |
| PRAGMA applications | 6 (9 on abort path) | **3** |
| journal/manifest/abort sub-op `opens` counters | 1 / 1 / 1 | **0 / 0 / 0** |

## 3. Behavior preservation

- **Transaction boundaries**: unchanged — every sub-op keeps its own explicit
  IMMEDIATE transaction (journal) / transaction (manifest); no transaction is
  held across `append` or between steps.
- **Visibility**: the manifest commit remains the last step of `commit` and
  the single visibility point.
- **Lock-wait**: the held connection carries no open transaction and no
  un-finalized statement between steps, so WAL checkpoints and concurrent
  readers/writers are never blocked by it.
- **Recovery/publication semantics**: the abort path's failure behavior is
  unchanged in effect — before, an abort-time open failure silently left the
  journal row for the next `recover` pass; now, an abort-time statement
  failure on the reused connection does the same (documented in-code). The
  #291 witnesses hold: verify_snapshot, abort, and corruption-detection
  behavior byte-for-byte unchanged (full suite, both feature modes, green:
  130 + the neutrality test each).
- **Instrumentation truthfulness**: sub-op `DbOp` events report
  `open_ns: 0`/`opens: 0` (no open happens inside them); the operation's one
  open is attributed by the `ConnectionOpen` event alone, keeping the
  #291-era open-cost attribution coherent under reuse.

## 4. Verification

- `cargo test -p stillflow-storage` → 130 passed + 1 neutrality, 0 failed.
- `cargo test -p stillflow-storage --features storage-metrics` → same result.
- `cargo fmt -p stillflow-storage -- --check` clean;
  `cargo clippy -p stillflow-storage --all-targets` zero warnings.
- Full-timing remeasurement was deliberately **not** run: the acceptance
  criterion is the open/PRAGMA **count** reduction at the final head, which
  the deterministic counting witness above provides; wall-clock deltas at
  ~0.4–1.1 ms per publication are below the O0-B1 §7 sub-second noise floor
  and would not be claimable per its rules.

## 5. Rollback point

Single commit (`perf(storage): reuse one SQLite connection per snapshot
publication`); reverting it restores the per-touchpoint connection lifecycle
exactly. No schema, format, API, or error-message change anywhere in the diff.
