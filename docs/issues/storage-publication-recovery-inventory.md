# S0-D0: SnapshotStore publication and recovery inventory

> Issue: #61
> Scope: fact inventory only; no new API design
> Evidence baseline: `main@85502cbebb1fab461fe42d30fe019ad20613aa7c`
> Storage crate: `backend/crates/stillflow-storage`
> Core contract crate: `backend/crates/stillflow-core`

This inventory records only behavior present at the evidence baseline. PR #53, #57, #59, and #60 are read-only references for this task. No behavior proposed by those PRs is treated as capability on `main`.

## 1. Current storage objects

| Object | Exact source path | Fields at baseline | Owning crate | Created by | Persisted? | Write / read entry | Lifecycle endpoint |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `SnapshotStore` | `backend/crates/stillflow-storage/src/store.rs` | `inner: Arc<StoreInner>`; `StoreInner` contains `root: PathBuf`, `limits: StorageLimits`, `_root_lock: File`, `activity: Mutex<ActivityState>` | `stillflow-storage` | `SnapshotStore::open(root, limits)` | The Rust object is in-memory. `open` owns root `.stillflow.lock` for the store lifetime and opens `metadata.sqlite3`; snapshot state is persisted beneath the managed root. | Writes through `begin_snapshot`, `recover`, `tombstone_snapshot`, `collect_garbage`; reads through `load_manifest`, `read_batches`, `verify_snapshot`. | Dropping the last `Arc<StoreInner>` closes the root lock file and releases the process ownership lock. |
| `SnapshotDraft` | `backend/crates/stillflow-storage/src/manifest.rs` | `id`, `dataset_id`, `session_id`, `source_asset_id`, `schema`, `schema_fingerprint`, `lineage`, `quality_score`, `created_at` | `stillflow-storage` | `SnapshotDraft::try_new` | No standalone persistence. Its values become `DatasetSnapshot` / SQLite snapshot columns only after successful commit. | Supplied to `SnapshotStore::begin_snapshot`; consumed by `SnapshotWriter::commit`. | Ends when the writer is dropped or consumed by `commit`. |
| `SnapshotWriter` | `backend/crates/stillflow-storage/src/store.rs` | `inner`, `_activity`, `draft`, `staging_dir`, `staged`, `next_input_sequence`, `envelope_count`, `row_count`, `stored_byte_count`, `installed`, `committed`, `failed` | `stillflow-storage` | `SnapshotStore::begin_snapshot` | Transient. It causes a `publications` journal row and filesystem staging/final files, but the writer itself is not serialized. | `append(&BatchEnvelope)` writes staged Parquet; `commit()` installs partitions and commits the SQLite manifest. | Successful `commit` marks `committed`; Drop after successful commit leaves the committed snapshot and final partitions intact; the publication journal row has already been deleted by the manifest transaction. Any non-committed drop best-effort removes staging, removes installed final directory when `installed`, and deletes the publication journal. The publisher activity guard lives for the whole writer lifetime. |
| `SnapshotManifest` | `backend/crates/stillflow-storage/src/manifest.rs` | `snapshot: DatasetSnapshot`, `partitions: Vec<SnapshotPartition>` | `stillflow-storage` | `SnapshotWriter::commit` builds it with `build_snapshot` + `SnapshotManifest::try_new`; `load_manifest_inner` reconstructs it. | There is no standalone manifest file. Snapshot fields are stored in SQLite `snapshots`; partition metadata is stored in SQLite `partitions`; Parquet payloads live under `partitions/<snapshot_id>/`. | Written by `commit_manifest`; read by `load_manifest`, `read_batches`, `verify_snapshot`. | Immutable value returned to callers or held by a reader. SQLite visibility ends when snapshot is tombstoned/deleted by storage maintenance. |
| `SnapshotBatchReader` | `backend/crates/stillflow-storage/src/store.rs` | `inner`, `_activity: ActivityGuard`, `manifest`, `next_partition` | `stillflow-storage` | `SnapshotStore::read_batches` | No. | Loads one visible manifest, then its `Iterator::next` reads one manifest partition at a time. | Holds a reader activity guard until the reader is dropped, including while the caller is between `next()` calls. |
| `StorageLimits` | `backend/crates/stillflow-storage/src/manifest.rs` | `max_input_envelopes`, `max_partitions`, `max_rows`, `max_stored_bytes`, `max_active_readers`, `max_active_publishers` | `stillflow-storage` | `Default` or `StorageLimits::try_new` | No dedicated persistence. The chosen limits live in `StoreInner` and are re-applied on reads/writes. | Read through `SnapshotStore::limits` and internal checks. | Same in-memory lifetime as the store configuration. |
| `IngestionEvent` | `backend/crates/stillflow-core/src/events/mod.rs` | `id`, `session_id`, `object_kind`, `object_id`, `relationship`, `timestamp`, `metadata`, `error` | `stillflow-core` | `IngestionEvent::try_new` and `ObjectEventMapper` helpers, including `snapshot_materialized` | Not persisted by `SnapshotStore`; the storage schema has no ingestion-event table and storage publication does not emit one. | No `SnapshotStore` write/read entry. It is a serializable core event value for connector/application consumers. | Ends with the owning consumer unless another layer persists it; no persistence path is present in this storage implementation. |

Supporting physical object: `SnapshotPartition` in `backend/crates/stillflow-storage/src/manifest.rs` has `sequence`, `row_count`, `stored_byte_count`, and `digest: ContentDigest`.

### 1.1 Managed-root persistence layout

At this baseline the relevant paths are constructed in `backend/crates/stillflow-storage/src/store.rs`:

- root ownership lock: `<root>/.stillflow.lock`;
- metadata database: `<root>/metadata.sqlite3`;
- staging root: `<root>/staging`;
- final partition root: `<root>/partitions`;
- one snapshot final directory: `<root>/partitions/<snapshot_id>`;
- one staged snapshot directory: `<root>/staging/<snapshot_id>`;
- staged partition filename: zero-padded ten-digit sequence plus `.parquet`, for example `<root>/staging/<snapshot_id>/0000000000.parquet`;
- final partition filename: zero-padded ten-digit sequence, `-`, the lowercase 64-character partition SHA-256 digest, then `.parquet`, for example `<root>/partitions/<snapshot_id>/0000000000-<sha256>.parquet`.

## 2. Actual publication order

The real order differs from a simplified `begin -> staging -> append -> install -> SQLite` description because `begin_snapshot` commits a SQLite publication journal **before** creating the staging directory.

### 2.1 Begin

`SnapshotStore::begin_snapshot` in `backend/crates/stillflow-storage/src/store.rs` performs:

1. validates `draft.created_at() <= started_at`;
2. acquires a publisher `ActivityGuard`;
3. calls `insert_publication`;
4. `insert_publication` starts a SQLite `TransactionBehavior::Immediate`, rejects an existing snapshot or publication with the same id, inserts `(snapshot_id, started_at_utc)` into `publications`, then commits that transaction;
5. creates `<root>/staging/<snapshot_id>`;
6. if staging-directory creation fails, best-effort `abort_publication` deletes the journal row;
7. returns `SnapshotWriter` holding the publisher guard.

Therefore a crash can leave a publication row even before a staging directory exists. The SQLite connection is configured with `journal_mode = WAL` and `synchronous = FULL`; this inventory records the committed transaction boundary but does not claim tested process-kill or power-loss durability.

### 2.2 Append

`SnapshotWriter::append` validates envelope count, exact input sequence, source asset identity, schema fingerprint **and complete schema equality**, partition count, row count, and stored-byte limits.

For a non-empty envelope, `write_partition`:

1. writes one Parquet file to the staging directory;
2. finalizes the `ArrowWriter`;
3. calls `File::sync_all` on the staged Parquet file;
4. reads its file length;
5. seeks to byte offset zero;
6. computes `ContentDigest` over the complete finalized Parquet bytes;
7. records a `SnapshotPartition` whose sequence is `self.staged.len()` converted to `u32`.

Zero-row envelopes advance input-envelope sequencing but create no physical partition.

### 2.3 Commit and visibility point

`SnapshotWriter::commit` performs:

1. builds `SnapshotStats`, `DatasetSnapshot`, then `SnapshotManifest` in memory;
2. creates `<root>/partitions/<snapshot_id>` and sets `installed = true`;
3. `install_partitions` renames each staged Parquet file into that final directory with `fs::rename`;
4. after all renames, `sync_directory(final_dir)` and `sync_directory(partitions_root)` run on Unix; `sync_directory` is a no-op under `#[cfg(not(unix))]`;
5. `commit_manifest` opens `metadata.sqlite3` and begins one SQLite `TransactionBehavior::Immediate`;
6. that transaction verifies the `publications` journal row exists;
7. inserts the visible snapshot row into `snapshots` and all partition metadata rows into `partitions`;
8. deletes exactly one matching `publications` row **inside the same transaction**;
9. commits the transaction;
10. only after that successful return, the writer sets `committed = true`;
11. it best-effort removes `<root>/staging/<snapshot_id>`.

The database visibility point is the successful SQLite transaction commit in step 9. Physical final Parquet files exist before that point.

Actual sequence:

```text
publisher gate
-> SQLite publications journal commit
-> staging directory create
-> staged Parquet append + file sync + SHA-256
-> final snapshot directory create
-> staged Parquet rename into final directory
-> directory sync on Unix / no-op on non-Unix
-> SQLite IMMEDIATE transaction
   -> verify journal
   -> insert snapshots row
   -> insert partitions rows
   -> delete journal row
   -> COMMIT  <-- manifest becomes visible here
-> mark writer committed
-> best-effort staging cleanup
-> writer drop releases publisher gate
```

No code path installs a standalone manifest file.

## 3. Concurrency, maintenance gate, and root lock

### 3.1 Publisher and reader permits

There is no `Semaphore` in this storage mechanism. `StoreInner.activity: Mutex<ActivityState>` contains counters:

```text
readers: u16
publishers: u16
maintenance: bool
```

`acquire_activity` fails immediately with `StorageError::Busy` rather than waiting.

Default limits from `backend/crates/stillflow-storage/src/manifest.rs` are:

- `MAX_ACTIVE_READERS = 64`;
- `MAX_ACTIVE_PUBLISHERS = 8`.

`StorageLimits::try_new` may lower either value but rejects zero and values above those maxima. A publisher permit is acquired before the publication-journal insert and is held through the complete `SnapshotWriter` lifetime, including append, install, SQLite commit, and commit cleanup. A reader permit returned by `read_batches` is held for the complete `SnapshotBatchReader` lifetime.

### 3.2 Maintenance gate

The maintenance gate is real and shared by `recover` and `collect_garbage`.

`acquire_maintenance` returns `Busy("storage activity prevents maintenance")` when:

- maintenance is already active;
- any reader is active;
- any publisher is active.

After it succeeds, `maintenance = true`; later reader/publisher acquisition returns `Busy("maintenance is active")`. `MaintenanceGuard::drop` restores `maintenance = false`.

Consequently `recover` cannot run concurrently with a normal reader or publisher through the same `StoreInner`.

### 3.3 Managed-root OS lock

`SnapshotStore::open` opens `<root>/.stillflow.lock` and calls `fs2::FileExt::try_lock_exclusive`. `WouldBlock` becomes `StorageError::Busy("managed root is already owned")`. The file handle remains in `StoreInner._root_lock` for the store lifetime.

This prevents a second successfully opened `SnapshotStore` from owning the same managed root concurrently. The in-memory maintenance gate and the root file lock therefore cover different scopes: activity exclusion inside one store owner, and managed-root ownership across opens/processes.

No separate `publisher` file lock exists.

## 4. Recovery implementation

`SnapshotStore::recover(now, stale_after, max_candidates)` in `backend/crates/stillflow-storage/src/store.rs`:

1. validates `max_candidates` against `MAX_MAINTENANCE_CANDIDATES = 1024`;
2. acquires the maintenance gate;
3. computes a cutoff;
4. queries `publications` where `started_at_utc <= cutoff`, ordered by `started_at_utc, snapshot_id`, bounded by `max_candidates`;
5. for each stale publication:
   - if `snapshot_is_visible` is true, removes only its staging directory;
   - otherwise removes both staging and final partition directories;
   - deletes the publication row;
6. scans bounded orphan staging directories; if no matching publication row exists, removes the orphan staging directory.

The recovery decision is therefore based on the SQLite publication journal plus current visible-snapshot state, not on a filesystem transaction marker.

`SnapshotWriter::drop` provides best-effort in-process abort cleanup, but it is not crash recovery because process termination does not run Rust destructors.

## 5. Crash-window matrix

Status vocabulary in this table is restricted to `proven`, `tested`, `implemented-but-untested`, `missing`, and `unknown`.

| Crash position | Disk residue | SQLite state | Current recovery status | Evidence at `main@85502cb` |
| --- | --- | --- | --- | --- |
| before `begin_snapshot` | No per-snapshot staging/final files created by this operation. | No publication or snapshot row created by this operation. | proven | `begin_snapshot` is the first publication entry and no pre-begin recovery state exists. `backend/crates/stillflow-storage/src/store.rs`. |
| after publication journal commit, before staging directory creation | No snapshot staging/final directory is required to exist yet. | `publications(id, started_at_utc)` is committed; no visible snapshot. | implemented-but-untested | `begin_snapshot` commits `insert_publication` before `create_exact_directory`. For an invisible stale publication, `recover` calls directory removal for staging and final; missing directories are tolerated, then the publication row is deleted. No test isolates this exact crash point. |
| after staging creation | `<root>/staging/<id>` may exist and can be empty. | `publications(id, started_at_utc)` is already committed; no visible snapshot. | implemented-but-untested | `begin_snapshot` commits `insert_publication` before `create_exact_directory`; `recover` removes staging/final for an invisible stale publication. No test isolates a process failure immediately after directory creation. |
| after partition append | Synced Parquet files under staging; no required final files yet. | Publication row present; no visible snapshot. | implemented-but-untested | `write_partition` syncs each staged file; invisible stale-publication recovery removes staging and final directories. Existing recovery test does not isolate this exact point. |
| before install | Staged Parquet files and publication row; final directory may not yet exist. | Publication row present; no visible snapshot. | implemented-but-untested | Same invisible-publication branch in `recover`; no separate install-boundary crash injection. |
| during multi-partition install after only some renames | Some partition files remain under staging; already-renamed partitions are under final. | Publication row present; no visible snapshot row because `commit_manifest` has not started. | implemented-but-untested | `install_partitions` iterates manifest partitions and performs one `fs::rename` per partition. Recovery of an invisible stale publication removes both staging and final snapshot directories, covering both halves of a partial install. No test injects a crash between individual renames. |
| after install, before SQLite manifest transaction | Final directory and renamed Parquet files exist; staging directory may remain. | Publication row present; no visible snapshot row. | tested | `recovery_removes_precommit_files_and_preserves_committed_snapshot` white-box creates final directory, calls `install_partitions`, suppresses writer Drop cleanup, verifies the snapshot is not visible, then calls `recover` and verifies final/staging removal. |
| during SQLite manifest transaction | Installed final files exist; staging may remain. | Transaction contains snapshot/partition inserts and publication deletion until commit. Observable post-crash DB state is not failure-injected by tests at this exact point. | implemented-but-untested | All DB visibility mutations are grouped in one `TransactionBehavior::Immediate`; no test terminates/reopens the process during this transaction. Recovery has paths for an invisible stale publication and for post-commit orphan staging, but mid-transaction restart is not directly exercised. |
| after DB commit, before cleanup | Final Parquet files are installed; staging residue may remain. | Snapshot and partitions are visible; publication row has been deleted in the same committed transaction. | tested | `recovery_removes_precommit_files_and_preserves_committed_snapshot` creates post-commit staging residue, calls `recover`, verifies the committed manifest/final directory remain and staging is removed. |
| while a reader is open | Reader itself writes no disk state; it may hold open/accessed partition files through reads. | No reader row or reader transaction is persisted by the storage protocol. | proven | `SnapshotBatchReader` holds `ActivityGuard`; `acquire_maintenance` rejects any nonzero reader count. Recovery therefore cannot overlap that reader through the same store. |

The recovery unit test is a same-process white-box state construction. It is not a restart-recovery test and does not kill a process.

## 6. Digest and identity facts

### 6.1 `ContentDigest`

Source: `backend/crates/stillflow-storage/src/digest.rs` plus `write_partition` in `store.rs`.

- Representation: exactly `[u8; 32]`.
- Algorithm: SHA-256 from `sha2::Sha256`.
- Input: the complete **finalized staged Parquet file bytes**, read from byte offset zero after `ArrowWriter::into_inner` and `File::sync_all`.
- Read order: byte order on disk, sequentially from offset zero to EOF.
- Hasher chunk size: `DIGEST_BUFFER_BYTES = 64 * 1024`; chunking adds no framing bytes.
- Text encoding: lowercase hexadecimal, two hex digits per digest byte, exactly 64 characters.
- Partition metadata stores the value in SQLite `partitions.sha256`.

It is not a digest of row values, schema JSON, snapshot id, or manifest fields.

### 6.2 `schema_fingerprint`

Source: `backend/crates/stillflow-core/src/batch.rs`.

- Algorithm label: `stillflow-schema-fnv1a64x4-v1`.
- It is explicitly a non-security index, not a cryptographic checksum.
- Input bytes: `serde_json::to_vec(schema)` after `LogicalSchema::validate()` succeeds.
- The schema serializer emits the current `LogicalSchema` representation; field vector order is preserved and its metadata maps are `BTreeMap` values.
- Hash state consists of four `u64` lanes initialized to:
  - `0xcbf29ce484222325`;
  - `0x6c62272e07bb0142`;
  - `0x9e3779b97f4a7c15`;
  - `0xd6e8feb86659fd93`.
- For every serialized JSON byte, in byte order, each lane performs `lane ^= byte ^ (lane_index << 8)` and then `lane = lane.wrapping_mul(0x00000100000001b3)`.
- Output encoding concatenates each lane's `to_be_bytes()` result in lane order to produce 32 bytes; display is lowercase 64-character hex.
- Storage append checks both this fingerprint and complete `LogicalSchema` equality, so fingerprint equality alone is not accepted as schema identity.

### 6.3 Snapshot and dataset identities

`SnapshotDraft::try_new` receives `snapshot_id`, `dataset_id`, `session_id`, and `source_asset_id` as caller-provided UUIDs. It rejects nil UUIDs. The storage layer does not derive `snapshot_id` or `dataset_id` from content digests, timestamps, schema fingerprints, or partition bytes.

Publication identity is `snapshot_id`: it is the primary key of `publications` and `snapshots`, and the directory name beneath staging/final roots.

### 6.4 Partition sequence

A non-empty appended envelope gets `partition_sequence = u32::try_from(self.staged.len())`. `SnapshotManifest::try_new` requires partition sequences to equal their zero-based vector indices and therefore be contiguous.

The two physical path encodings are different:

- staging: `<root>/staging/<snapshot_id>/<sequence:010>.parquet`, for example `<root>/staging/<snapshot_id>/0000000000.parquet`;
- final: `<root>/partitions/<snapshot_id>/<sequence:010>-<sha256>.parquet`, for example `<root>/partitions/<snapshot_id>/0000000000-<sha256>.parquet`, where `<sha256>` is the lowercase 64-character `ContentDigest` of the finalized Parquet file.

Input envelope sequence is a separate `u64` and must equal `SnapshotWriter.next_input_sequence`; empty envelopes consume input sequence numbers but do not consume partition sequence numbers.

### 6.5 Manifest versions

There is no independent `version` field on `SnapshotManifest`.

- `DatasetSnapshot.version` is `DATASET_SNAPSHOT_VERSION = 1` from `backend/crates/stillflow-core/src/domain/snapshot.rs`.
- The storage metadata schema uses `STORAGE_SCHEMA_VERSION = 1` from `backend/crates/stillflow-storage/src/manifest.rs`.
- `snapshots.version` is persisted and checked as version 1.

These two version domains should not be collapsed into a fictional `SnapshotManifest` version.

## 7. Test evidence

No workspace tests were run for this inventory. This table records tests present in the baseline source.

| Mechanism | Test file | Exact test function | Failure/state injection | Restart recovery? | Platform scope / Windows fact |
| --- | --- | --- | --- | --- | --- |
| storage migration/version handling | `backend/crates/stillflow-storage/src/store.rs` | `migration_is_idempotent_and_future_versions_fail_closed` | Direct metadata-version state manipulation | No | No platform cfg on test. |
| root ownership lock | same | `managed_root_has_one_independent_owner` | Attempts a second `SnapshotStore::open` while first owner is alive, then opens again after owner drop | No recovery restart | No platform cfg on test; lock uses `fs2::FileExt`. |
| read-time manifest bounds | same | `manifest_loading_reapplies_configured_and_batch_bounds` | Manipulates persisted manifest data beyond accepted bounds | No | No platform cfg on test. |
| pre-commit invisibility and normal commit | same | `snapshot_is_invisible_until_commit_and_roundtrips_exactly` | Observes snapshot before and after normal commit | No | No platform cfg on test. |
| empty-envelope physical behavior | same | `empty_envelopes_create_no_physical_partitions` | Empty batch envelopes | No | No platform cfg on test. |
| ordered partition round-trip | same | `alternate_batch_partitions_preserve_ordered_rows` | Alternate batch partitioning | No | No platform cfg on test. |
| sequence/lineage/schema/resource rejection | same | `rejects_sequence_lineage_schema_and_configured_bounds` | Invalid sequence, lineage, schema, configured limits | No | No platform cfg on test. |
| digest/missing-file/lazy-reader fail-closed behavior | same | `checksum_missing_file_and_lazy_reader_fail_closed_without_paths` | Corrupts partition bytes and removes files | No | No platform cfg on test. |
| symlink partition rejection | same | `symlinked_partition_fails_closed` | Replaces/uses a symlinked partition path | No | `#[cfg(unix)]`; this test does not run on Windows. |
| pre-commit installed-file recovery and post-commit staging cleanup | same | `recovery_removes_precommit_files_and_preserves_committed_snapshot` | White-box constructs an installed-but-uncommitted state and a committed snapshot with staging residue | **No**; same `SnapshotStore` remains open | No platform cfg on test. Directory durability differs because `sync_directory` is Unix-only. |
| tombstone/GC/activity safety | same | `tombstone_retention_gc_and_activity_are_safe` | Tombstone ages and active-storage conditions | No | No platform cfg on test. |
| configured activity caps | same | `configured_activity_limits_fail_fast` | Limits readers/publishers to 1 and verifies fail-fast `Busy` behavior | No | No platform cfg on test. |
| timestamp/limit validation | same | `timestamp_and_limit_validation_are_explicit` | Invalid limits and timestamp order | No | No platform cfg on test. |
| digest text codec | `backend/crates/stillflow-storage/src/digest.rs` | `digest_hex_roundtrips` | Invalid digest length and invalid hex | No | No platform cfg on test. |
| logical snapshot serialization/fingerprint validation | `backend/crates/stillflow-core/src/domain/snapshot.rs` | `stable_snapshot_roundtrips_with_logical_schema_only`; `deserialization_revalidates_fingerprint_and_schema` | JSON round-trip and mismatched fingerprint/schema | No | No platform cfg on tests. |

### 7.1 Failure-injection and restart limits

The baseline has targeted state manipulation and file-corruption tests, but no test in `stillflow-storage` at this commit performs an actual process kill between publication phases and then opens a fresh process/store to recover the root. The existing recovery test proves same-process recovery logic against constructed disk/SQLite states; it does not prove power-loss durability or restart orchestration.

### 7.2 Unix / Windows differences visible in source

Two source-level differences are explicit:

1. `sync_directory(path)` opens and `sync_all`s directories under `#[cfg(unix)]`, while `#[cfg(not(unix))]` returns `Ok(())` without a directory sync.
2. `symlinked_partition_fails_closed` is compiled only on Unix.

No Windows-specific crash/recovery test is present in this storage module at the baseline. The same `fs::rename` publication code is compiled cross-platform, but this inventory does not infer stronger crash guarantees than the tests and cfg branches establish.

## 8. Fact mapping to PR #57 proposals

PR #57 at read-only head `2a35bced9e2eb8b35a9e4679c8698d15bbb6b941` proposes a `VerificationBundle` publication model. The table below maps only whether the named capability exists on `main@85502cb`; it does not adopt or modify the proposal.

| #57 proposal | Current `main@85502cb` capability | Gap |
| --- | --- | --- |
| `VerificationBundle` | `SnapshotStore` publishes one `SnapshotManifest` / `DatasetSnapshot` per writer. No `VerificationBundle` type or bundle publication row exists in the storage baseline. | missing |
| multi-Artifact atomic publication | One commit transaction atomically makes **one snapshot manifest plus its partition metadata** visible after its Parquet files are installed. There is no transaction spanning accepted snapshot + report artifacts + rejected rows + provenance. | missing |
| bundle reader | `SnapshotBatchReader` reads one snapshot manifest's Parquet partitions. There is no bundle manifest loader or artifact-section reader on main. | missing |
| maintenance recovery | `SnapshotStore::recover` exists, uses the in-memory maintenance gate, root ownership lock, stale `publications` journal, visible snapshot check, and orphan-staging scan. Its recovery unit is one snapshot id, not a bundle. | implemented-but-untested |
| dedup temporary-file recovery | This storage implementation has no dedup temporary-file/index object, ownership journal, or dedup cleanup path. `recover` knows only snapshot publication directories/journal plus orphan staging. | missing |

For the `maintenance recovery` row, `implemented-but-untested` specifically means the baseline implements maintenance-gated snapshot recovery but has no true restart/crash test and no bundle-level recovery behavior. It does not mean the mechanism is absent.

## 9. Facts that directly constrain #57 R3 review

The following are baseline constraints, not solution proposals:

1. The publication-journal transaction is committed before staging-directory creation; SQLite connections use WAL with `synchronous = FULL`, while actual process-kill and power-loss recovery remain untested.
2. Physical final Parquet installation precedes SQLite snapshot visibility.
3. Snapshot visibility and deletion of the publication journal occur in the same SQLite transaction.
4. Existing recovery treats a stale publication with no visible snapshot as unpublished and removes both staging and final partition directories.
5. Post-commit staging residue is recoverable even though the publication journal is already gone, through orphan-staging scanning.
6. A publisher guard spans the whole `SnapshotWriter`; maintenance cannot overlap it.
7. A `SnapshotBatchReader` guard spans the whole reader lifetime; maintenance cannot overlap it.
8. The managed root has one exclusive `.stillflow.lock` owner per successful `SnapshotStore::open` lifetime.
9. Directory synchronization is materially different between Unix and non-Unix source branches.
10. The existing recovery test is not a process-restart test.
11. `ContentDigest` hashes finalized Parquet file bytes; `schema_fingerprint` hashes serialized logical-schema JSON with a separate non-cryptographic algorithm. They are not interchangeable identity mechanisms.
12. Current storage has no multi-artifact or dedup-temp recovery object to reuse by name; only the maintenance/root-lock exclusion mechanism and single-snapshot publication journal are present.

## 10. Inventory closure

This document is bound to `main@85502cbebb1fab461fe42d30fe019ad20613aa7c`. It records current implementation and test evidence only. It does not change PR #57, freeze a bundle protocol, add a storage API, or authorize E4 runtime work.