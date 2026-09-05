# O0-S1 — Parquet write digest and SQLite connection lifecycle costs

- Version: 1 (evidence note, issue #286)
- Date: 2026-09-05
- Exact measured head: `d666a9de56efd828683f977e3af6d985912e93d1`
  (branch `agent/issue-286-o0-s1-storage-costs`)
- Dispatch base: `main@f61e0853b67ff5ca7bedb0bddb707befb922baff`
- Scope: measurement and opt-in instrumentation only. No persistence bytes,
  digest algorithm, schema, migration, transaction boundary, publication,
  recovery, or production connection-lifetime behavior changed. The
  instrumentation is compiled only under the `storage-metrics` cargo feature
  (disabled by default); with the feature off, `cargo test -p
  stillflow-storage` passes unchanged and a dedicated neutrality test
  (`tests/o0s1_metrics_neutrality.rs`) asserts that nothing is recorded and
  that storage behavior (publication, corruption detection, abort) is
  byte-for-byte the same.
- Companion raw aggregate (verbatim probe output folded over the five runs):
  `docs/evidence/performance/o0-s1-raw-aggregate.md`

## 1. Machine, concurrency policy, and exact commands

Machine (from `/proc`, also emitted by the probe):

| item | value |
| --- | --- |
| CPU | 6 vCPU, 12th Gen Intel(R) Core(TM) i3-12100F |
| Kernel | Linux 6.18.33.2-microsoft-standard-WSL2 |
| RAM | `MemTotal` 12,249,204 KiB (WSL2, ~11.7 GiB) |
| OS | linux (WSL2) |
| Build | `--release`, page cache warm, storage on ext4 via WSL2 |

Concurrency policy: five O0 performance agents ran in parallel on this
machine. All cargo builds used the shared
`CARGO_TARGET_DIR=/home/owl/.cargo-o0-target` (serialized by cargo's own
lock, all outside the measurement lock). Every timed probe run took the
serializing lock `flock /tmp/stillflow-o0-measure.lock`, so at most one
measurement was executing at any instant; five full runs were taken per
metric. `/usr/bin/time -v` is not available in this environment; peak RSS is
the kernel high-water mark (`VmHWM` from `/proc/self/status`), sampled per
scenario by the probe and read at process exit, which is the same quantity
`/usr/bin/time -v` reports.

Exact commands (all run at the measured head; builds untimed, runs locked):

```sh
cd backend
export CARGO_TARGET_DIR=/home/owl/.cargo-o0-target

# Build (untimed setup)
cargo build --release -p stillflow-storage --features storage-metrics \
  --example o0s1_storage_cost_probe

# Timed measurement: five serialized runs under the O0 measurement lock
for i in 1 2 3 4 5; do
  flock /tmp/stillflow-o0-measure.lock -c \
    '/home/owl/.cargo-o0-target/release/examples/o0s1_storage_cost_probe
     --iterations 7 --b-op-iterations 30 --seq-iterations 15 --conc-ms 1500
     > /tmp/o0s1-run'"$i"'.jsonl'
done

# Aggregation (P50/P95/min/max + inter-run median spread)
python3 backend/crates/stillflow-storage/examples/o0s1_aggregate.py \
  /tmp/o0s1-run1.jsonl /tmp/o0s1-run2.jsonl /tmp/o0s1-run3.jsonl \
  /tmp/o0s1-run4.jsonl /tmp/o0s1-run5.jsonl

# Gates (untimed)
cargo fmt --all -- --check
cargo test -p stillflow-storage
cargo test -p stillflow-storage --features storage-metrics
```

The probe emits JSON lines (`sample` per iteration, `witness` for
correctness, `conc_sample` for per-operation concurrency latencies, `info`
for machine/mode facts). `o0s1_aggregate.py` folds all runs into the tables
summarized below. Instrumentation counts logical reads (bytes and passes
counted at the `read()` loop inside `digest_file` call sites), never device
I/O.

## 2. Part A — Parquet / artifact write attribution

### 2.1 Surfaces instrumented

`SnapshotStore::begin_snapshot -> SnapshotWriter::append -> commit` writes
one Parquet partition per envelope into `partitions/` staging via
`write_envelope_parquet` (`backend/crates/stillflow-storage/src/store.rs`);
the same writer function is the artifact partition writer for
`VerificationBundle` children (`backend/crates/stillflow-storage/src/bundle.rs`
calls `write_envelope_parquet` for accepted/rejected/report partitions), so
snapshot measurements attribute the artifact write path too. After
`into_inner()` the staged file is `sync_all()`-ed, stat-ed, rewound
(`seek(0)`), and **reread in full once** by `digest_file` (64 KiB read chunks
into SHA-256) — this is the digest reread the issue targets. Publication
then renames each staged partition into `partitions/<id>/` and fsyncs the
final directory and the partitions root (`install_partitions`), followed by
one SQLite manifest-commit transaction. The digest-preimage canonical
encoding (`canonical_batch_bytes`, Arrow IPC rebuild) is separately timed
via the `CanonicalBatch` event; it is the dominant digest/summary CPU on
artifact bundle commits and version-digest recomputation.

### 2.2 Fixtures

| fixture | rows | columns | shape | Parquet bytes written (P50) |
| --- | --- | --- | --- | --- |
| small | 1,000 | 4 | int64/float64/utf8/bool | 19,031 |
| medium | 65,536 | 8 | mixed fixed-width + utf8 | 2,781,937 |
| wide | 2,000 | 200 | 50 x (int64/float64/utf8/bool) | 1,695,288 |
| longvar | 65,536 | 6 | variable-length utf8/binary dominated, 10% nulls | 2,075,930 |

All rows fit one `MAX_BATCH_ROWS` (65,536) envelope = one partition, matching
the engine's one-envelope-per-partition publication pattern.

### 2.3 Attribution (P50 / P95 over 5 runs x 7 iterations = 35 samples, ns)

| phase | small | medium | wide | longvar |
| --- | --- | --- | --- | --- |
| staged file create | 3.9 us / 7.8 us | 4.0 us / 6.2 us | 4.1 us / 6.7 us | 4.0 us / 6.8 us |
| Parquet encode + finalize | 223.9 us / 392.2 us | 21,370.7 us / 28,899.6 us | 13,246.7 us / 16,273.2 us | 29,096.6 us / 45,272.4 us |
| file `sync_all` | 0.4 us / 0.6 us | 1.5 us / 2.0 us | 1.4 us / 1.7 us | 1.6 us / 3.2 us |
| stat stored bytes | 0.8 us / 1.8 us | 3.8 us / 5.5 us | 3.5 us / 4.4 us | 3.8 us / 6.2 us |
| rewind (`seek(0)`) | 0.5 us / 1.0 us | 1.0 us / 1.5 us | 0.9 us / 1.3 us | 1.0 us / 2.2 us |
| **digest reread (wall)** | **14.6 us / 23.8 us** | **1,955.9 us / 2,269.8 us** | **1,109.1 us / 1,265.2 us** | **1,401.5 us / 1,631.5 us** |
| writer total | 250.2 us / 447.8 us | 23,366.6 us / 30,867.5 us | 14,557.4 us / 17,534.6 us | 30,443.6 us / 46,963.6 us |
| install: renames (1) | 6.4 us / 14.4 us | 10.3 us / 18.9 us | 10.5 us / 17.4 us | 11.0 us / 25.3 us |
| install: 2 directory fsyncs | 3.0 us / 5.8 us | 7.6 us / 16.2 us | 7.6 us / 12.2 us | 7.7 us / 27.9 us |
| SQLite journal (begin) | 521.2 us / 839.4 us | 558.5 us / 867.6 us | 560.8 us / 1,031.8 us | 561.2 us / 921.4 us |
| SQLite manifest commit | 548.0 us / 1,015.7 us | 735.6 us / 1,126.2 us | 737.0 us / 1,231.9 us | 770.4 us / 1,216.2 us |
| **full publish (begin->commit)** | **1,608.0 us / 2,588.6 us** | **25,130.3 us / 33,088.4 us** | **16,771.5 us / 20,856.1 us** | **32,213.0 us / 49,455.6 us** |

Inter-run median spread: 8–34% for the millisecond-scale encode/total phases,
20–80% for the microsecond-scale metadata phases (expected scheduler noise),
0% for all byte/pass counts (deterministic).

### 2.4 Reread findings (measured logically, not inferred)

| fixture | bytes written | bytes reread for digest | read passes | reread/written |
| --- | --- | --- | --- | --- |
| small | 19,031 | 19,031 | 1 | 1.000 |
| medium | 2,781,937 | 2,781,937 | 1 | 1.000 |
| wide | 1,695,288 | 1,695,288 | 1 | 1.000 |
| longvar | 2,075,930 | 2,075,930 | 1 | 1.000 |

The writer path performs exactly **one** seek/rewind and exactly **one**
full logical read pass over exactly the stored bytes; reread bytes equal
written bytes for every fixture and every run (0% spread — this is a counted
invariant, not an estimate). The read path (`read_partition` verification)
performs the same one-pass digest reread plus the Parquet decode pass; in the
publish-read sequence each partition is reread for verification twice (once
by `read_batches`, once by `version_digest`), 38,062 bytes / 2 passes for the
small fixture.

### 2.5 Digest CPU vs wall contribution

Pure-CPU SHA-256 calibration over the same 64 KiB chunk pattern as
`digest_file`: **0.528–0.567 ns/byte across the five runs (mean 0.539)**.

| fixture | reread bytes | digest CPU est. (bytes x 0.539 ns) | digest wall P50 | CPU share of wall | digest share of writer total | digest share of full publish |
| --- | --- | --- | --- | --- | --- | --- |
| small | 19,031 | ~10.3 us | 14.6 us | ~70% | 5.8% | 0.9% |
| medium | 2,781,937 | ~1,499 us | 1,955.9 us | ~77% | 8.4% | 7.8% |
| wide | 1,695,288 | ~913 us | 1,109.1 us | ~82% | 7.6% | 6.6% |
| longvar | 2,075,930 | ~1,118 us | 1,401.5 us | ~80% | 4.6% | 4.4% |

Interpretation: the digest reread costs ~0.5–0.7 of pure-hash time per byte
in wall terms (the remainder is `read()` copies), and it is **never the
dominant write cost** — Parquet encoding dominates (62–96% of writer total).
The metadata phases (stat, rewind, create, `sync_all`, renames, directory
fsyncs) are all single-digit microseconds on this filesystem.

### 2.6 Peak RSS and resources (Part A)

| scenario | VmHWM P50 (KiB) | spread |
| --- | --- | --- |
| a.write.small | 94,788 | 0.4% |
| a.write.medium / wide / longvar | 95,632 | 0.3% |
| probe process at exit (after all scenarios incl. concurrency buffers) | 201,504 | 1.9% |

A single-partition snapshot publish holds one buffered Parquet writer, the
input envelope, and one 64 KiB digest buffer; RSS scales with fixture
logical size (~35 MiB for the 65,536-row fixtures) and is unaffected by the
instrumentation (no recorded value feeds a code path).

### 2.7 Correctness witnesses (exact head)

For every run and every fixture the probe re-reads the published partition
file from disk outside the storage API and compares against the manifest:

- external SHA-256 over file bytes == `SnapshotPartition.digest()` hex
  (`digest_match: true` — all 4 fixtures x 5 runs);
- file length == manifest `stored_byte_count` (`size_match: true`);
- `store.verify_snapshot(id)` passes (`verify_ok: true`);
- committed logical `version_digest` present and stable per fixture.

Failure/recovery witnesses (measurement does not bypass atomic publication):

- `a.fail.drop`: after `begin_snapshot` + `append` (staged write recorded:
  `parquet_write_count = 1`), dropping the writer without `commit` removed
  the staging directory (`staging_removed_after_drop: true`), left no visible
  manifest (`manifest_not_found: true`), recorded **no**
  `PartitionInstall` and **no** manifest-commit event, and
  `SnapshotStore::recover` afterwards reports `examined = 0, recovered = 0`
  (publication journal was already aborted). Measured bytes never became
  visible and never bypassed the rename/commit sequence.
- `a.fail.corrupt`: flipping one byte (offset 9515, mid-file) of a published
  partition makes `verify_snapshot` and the batch-read path fail closed with
  `Integrity(DigestMismatch)` while the verification reread is still
  measured — corruption detection is fully preserved with instrumentation
  on.

## 3. Part B — SQLite connection lifecycle attribution

### 3.1 Surfaces instrumented

Every storage/control-plane operation calls `open_connection`
(`backend/crates/stillflow-storage/src/store.rs`), which creates a fresh
`Connection::open`, applies `busy_timeout(5000)`, and executes the frozen
PRAGMA batch `foreign_keys=ON; journal_mode=WAL; synchronous=FULL` — three
PRAGMA applications per connection, per operation. Connections are dropped
at operation end; there is no pooling today (unchanged). Representative
operations instrumented: `load_manifest` (read), `create_dataset`
(control-plane autocommit write), publication journal + manifest commit
(explicit IMMEDIATE transactions), abort cleanup.

### 3.2 Single-operation attribution (P50 / P95 over 5 runs x 30 = 150 samples)

| metric | load_manifest | create_dataset |
| --- | --- | --- |
| operation wall | 549.6 us / 903.6 us | 569.8 us / 794.5 us |
| `open_connection` total | 441.7 us / 677.7 us | 441.3 us / 629.3 us |
| — `sqlite3_open` | 36.6 us / 58.8 us | 36.8 us / 53.0 us |
| — configure (busy timeout + 3 PRAGMAs) | 403.6 us / 633.6 us | 400.1 us / 589.6 us |
| statements | 42.0 us / 105.7 us | 59.6 us / 101.7 us |
| transaction begin / commit | 0 (read) / 0 | 0 (autocommit) / 0 |
| opens per operation | 1 | 1 |
| PRAGMA applications | 3 | 3 |
| **open+configure share of wall** | **80.4%** | **77.5%** |

`create_dataset` uses no explicit transaction: every statement autocommits
under `synchronous = FULL`, so its per-statement commit/fsync cost is folded
into the 59.6 us statement phase (the fsync itself is not separately visible
without changing the code path; noted, not measured).

The configure phase dominates the open: re-applying `journal_mode=WAL` on
every new connection costs ~0.4 ms per operation even though the database is
already in WAL mode. On freshly written databases (write scenarios) the same
phase measures ~0.87–1.14 ms, i.e. the PRAGMA cost grows with recent write
activity.

### 3.3 Realistic short sequence (publish -> read -> verify), 75 samples

Per sequence: `begin_snapshot` + 1 append + `commit` + `load_manifest` +
`read_batches` (1 partition) + `version_digest`.

| metric | P50 / P95 |
| --- | --- |
| sequence wall | 3,849.2 us / 5,121.3 us |
| connection opens per sequence | **5** |
| PRAGMA applications per sequence | **15** |
| `open_connection` wall (all 5) | 2,635.2 us / ~3,700 us |
| open + configure (event-summed) | 2,379.6 us / 3,154.3 us (**61.8% of wall**) |
| transaction begin + commit (both explicit txns) | 2.9 us + 17.0 us + 23.8 us / ~90 us |
| manifest statement work (all ops) | 273.0 us / ~440 us |
| verify digest rereads | 2 passes, 38,062 bytes, 28.9 us |

The five opens are: publication journal insert, manifest commit, and three
`load_manifest` calls (explicit reload, `read_batches`, and
`version_digest`). Reused bytes: the same 19 KB partition is read from disk
twice more for digest verification inside one sequence.

### 3.4 Lock/busy behavior under existing supported concurrency

Configuration (all inside the repo's existing limits; nothing new introduced):
1 store, 4 reader threads looping `load_manifest`, 2 publisher threads
looping `begin_snapshot -> append -> commit` (small fixture), 1.5 s per run,
WAL mode, frozen 5,000 ms busy timeout, default `StorageLimits` (64 readers /
8 publishers).

| metric | value across 5 runs |
| --- | --- |
| reader ops observed | 852–978 per run (4,529 samples total) |
| reader `load_manifest` P50 / P95 | 6,460.4 us / 8,887.9 us (vs 549.6 us / 903.6 us isolated — ~11.8x P50 contention) |
| publisher full publications P50 / P95 | 14,094.7 us / 18,674.4 us (vs 1,608.0 us isolated small publish) |
| app-level `Busy` errors (readers / publishers) | **0 / 0** |
| storage-level errors | **0** |
| peak SQLite-related open fds (10 ms sampling) | **13** |
| peak total open fds | **19** |

Interpretation: within the supported concurrency envelope WAL plus the busy
timeout absorb all contention (no Busy surfaced at either the activity-guard
or SQLite layer); the cost appears as latency amplification (~12x readers,
~8.8x publishers) rather than failures. Peak sampled concurrent SQLite
connections (13) matches ~2 connections per active thread (the store's
short-lived connection pattern under 6 busy threads); logical opens remain
1 per operation by construction (counted, not sampled).

### 3.5 Resource use

- Peak RSS (Part B store with 8 seeded snapshots + concurrency buffers):
  ~95.6 MiB mid-run, 201.5 MiB process peak at exit (dominated by the probe's
  latency sample buffers, not SQLite).
- Every connection allocates SQLite page-cache/WAL bookkeeping for its
  lifetime (microseconds to milliseconds); no pooling exists, so fd use is
  bounded by the activity guards (`MAX_ACTIVE_READERS=64`,
  `MAX_ACTIVE_PUBLISHERS=8`), and sampled peaks stay far below that.

## 4. Analytical assessment (no implementation)

### 4.1 Writer-side streaming digest over the currently-reread bytes

Idea: hash the partition bytes as the Arrow writer emits them (wrap the
staged `File` in a hashing `Write` sink passed to `ArrowWriter::try_new`) so
the digest of exactly the canonical file bytes is available at
`into_inner()` — eliminating the rewind + full reread pass.

- Correctness/durability risks: LOW but non-zero. The streamed hash equals
  the file SHA-256 only if every byte handed to the sink is exactly what
  reaches the file and the footer is final before hashing stops. Any error
  mid-encode (partial file) must discard the streamed digest and keep the
  existing fail-closed behavior — the partition is never published with a
  digest that was not verified against final bytes. `into_inner()` ordering
  must guarantee hash flush after the last footer byte. Batching must not
  change the byte sequence (SNAPPY blocks, row-group layout, footer are
  produced identically today).
- Invalidation/lifetime rules: digest is final only after successful
  `into_inner()`; on any `StorageError::parquet`/io error the digest must be
  dropped and the staged file removed (already the abort path). No reuse of
  a streamed digest across retries.
- Extra memory/fd cost: one SHA-256 state (112 bytes) + no extra buffer if
  hashing taps the write path; zero extra file descriptors; actually saves
  one seek + one full read pass.
- Measured benefit ceiling: removes 0.9–8.4% of writer total (0.9–7.8% of
  full publish wall). Worth doing only where artifact writes are hot; it does
  not change the encoding-dominated profile.
- Required regression tests: (1) streamed digest == post-write file SHA-256
  for every fixture shape incl. wide/longvar and >1-row-group payloads;
  (2) encode-failure path discards the digest and leaves no published
  partition; (3) atomic publication unchanged (staging rename order, journal
  completion); (4) corruption of any published byte still fails
  `DigestMismatch`; (5) digest values unchanged vs today's reread digest for
  golden fixtures (exact persisted bytes law).

**Decision: GO (conditional).** The reread is a measured, removable, exactly-
one-pass cost with a low-risk implementation surface and a byte-exact
equality contract that is fully testable. Priority is modest because
encoding dominates; treat as a bounded L2 optimization, not urgent.

### 4.2 Operation-scope SQLite connection reuse

Idea: let one logical operation (or one `SnapshotWriter`/one
publish-read sequence) reuse a single configured connection instead of
opening+configuring per store call.

- Correctness/durability risks: MEDIUM — manageable if strictly scoped.
  A connection that outlives one store call can pin a WAL read snapshot if a
  statement is left un-finalized, delaying checkpoints and changing
  lock/busy timing; error paths (Integrity/database errors) leave statement
  state that must be reset before reuse; `PRAGMA journal_mode=WAL` and
  `synchronous=FULL` semantics must hold for the whole reused lifetime (they
  are per-connection, so reuse *preserves* them but skips re-application —
  the measured 0.4 ms/operation); rusqlite `Connection` is `Send`-not-`Sync`,
  so ownership must be documented.
- Invalidation/lifetime rules: acquire at logical-operation start, drop at
  operation end (including error paths via `Drop`); never reuse across
  transactions of different operations; never hold across
  filesystem publication phases unless the transaction is closed (the
  current `commit_manifest` transaction is the visibility point and must
  stay last); recovery/maintenance paths must keep their exclusive
  connection discipline.
- Extra memory/fd cost: one held connection per active operation (~1 fd +
  SQLite page-cache memory each, instead of 5 opens/sequence today); peak
  concurrent fds unchanged (bounded by activity guards) but held ~longer.
- Measured benefit: open+configure is **80.4%** of `load_manifest` wall,
  **77.5%** of `create_dataset` wall, and **~62%** of the publish-read
  sequence wall (5 opens, 15 PRAGMAs per sequence). Under concurrency the
  amplification is large (readers 11.8x P50), so per-op savings compound.
- Required regression tests: (1) per-connection PRAGMA state asserted after
  reuse (foreign_keys on, WAL, synchronous FULL); (2) WAL snapshot not pinned
  across operations (checkpoint proceeds while a reused connection is idle);
  (3) statement-reset/finalization on every error path; (4) busy/lock
  behavior byte-identical under the existing concurrency scenario (0 Busy
  today must stay 0; error kinds preserved); (5) publication visibility point
  unchanged (manifest commit still last, still IMMEDIATE); (6) fd-count
  ceiling unchanged under max activity guards.

**Decision: GO (conditional) for operation-scope reuse.** The measured
open/configure cost dominates single-op latency and the sequence, and a
strictly operation-scoped lifetime preserves every safety law with bounded
risk. **NO-GO for a general connection pool** at this time: the issue
 authorizes a pool only if operation-scope reuse proves insufficient, and
the sequence data (5 sequential opens on one thread) shows op-scope reuse
covers the dominant case; a pool adds cross-operation lifetime/invalidation
risk (stale WAL snapshots, PRAGMA drift, fd retention) with no measured need
yet. Re-evaluate only if a workload with genuinely concurrent same-database
short operations shows the open cost surviving op-scope reuse.

## 5. Acceptance criteria

- [x] Logical reread bytes/passes are measured, not inferred from device I/O
  alone — counted at the `digest_file` call sites (bytes written == bytes
  reread, exactly 1 pass, 0% spread).
- [x] Digest CPU/time contribution and total write time are both reported —
  calibrated CPU estimate + measured wall per phase, writer total, and full
  publish total (section 2.5).
- [x] SQLite open/PRAGMA cost is separated from query/transaction cost —
  open vs configure split per connection; per-op open/txn/statement/commit
  attribution (sections 3.2–3.3).
- [x] Representative operation sequences and concurrency are measured —
  publish-read sequence (5 opens/15 PRAGMAs) and mixed reader/publisher load
  within existing limits (section 3.4).
- [x] Measurement noise is reported — inter-run median spread per metric;
  deterministic counts show 0% (sections 2.3, tables).
- [x] No persistence bytes, digest, schema, migration, transaction,
  publication, recovery, or production connection lifecycle changes —
  feature-gated instrumentation, observations only; neutrality test
  `tests/o0s1_metrics_neutrality.rs` (passes with the feature on and off);
  full `cargo test -p stillflow-storage` green both ways; `cargo fmt
  --check` clean.
- [x] Exact-head correctness witnesses are included — external SHA-256,
  size, verify, version-digest witnesses, abort and corruption witnesses at
  head `d666a9de56efd828683f977e3af6d985912e93d1` (sections 2.7).

## 6. Deviations

- `/usr/bin/time -v` is unavailable in the measurement environment; peak RSS
  is taken from `/proc/self/status` `VmHWM` (same kernel high-water-mark
  quantity) via in-probe sampling plus exit read.
- Digest CPU is attributed via a calibrated pure-SHA-256 throughput
  measurement (same 64 KiB chunk pattern) rather than a per-thread CPU clock,
  which Rust std does not expose; calibration spread across runs was 0.528–
  0.567 ns/byte (< 7%).
- `create_dataset` per-statement implicit-commit (fsync) cost is noted
  analytically but not separately timed, because separating it would have
  required changing the autocommit code path (forbidden by this issue).
- The five parallel O0 agents share the machine; all measurements were
  serialized through `flock /tmp/stillflow-o0-measure.lock`, so runs are
  uncontended by sibling agents but background build activity may have
  contributed to the reported inter-run spread.
