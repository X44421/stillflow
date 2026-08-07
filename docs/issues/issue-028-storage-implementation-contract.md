# Issue #28 implementation contract: atomic snapshot storage

> Status: Frozen
> Risk: `risk:high`
> Base: `main@a06967ea5b1ce6037e155528bb44cd57349b05fd`
> Issue: https://github.com/X44421/stillflow/issues/28

## 1. Objective

Introduce the PR3 persistence boundary without pulling PR4 connector work or
PR5 engine/API work forward. This delivery must provide:

- a stable, logical `DatasetSnapshot` domain contract;
- a new `stillflow-storage` adapter crate;
- SQLite schema version 1 for snapshot manifests, partition records,
  publication journals, and tombstones;
- immutable Apache Parquet 59 partitions;
- durable, atomic snapshot publication;
- bounded integrity verification, crash recovery, tombstoning, and garbage
  collection;
- streaming reads back into validated `BatchEnvelope` values.

The storage crate is a local snapshot store, not a general database or object
storage abstraction.

## 2. Risk and compatibility decision

This work is high risk because it changes a public domain type, adds a workspace
crate and third-party dependencies, defines on-disk formats, and owns deletion
and crash-recovery behavior.

The physical `DatasetSnapshot` contract merged through Issue #5 is replaced,
not adapted. No compatibility shim is allowed for its old JSON shape.

The following public members are removed:

- `SchemaFieldSnapshot`;
- `DatasetSnapshot.storage_ref`;
- `DatasetSnapshot.schema_fields`;
- `DatasetSnapshot.schema_metadata`;
- `DatasetSnapshot.schema: Option<Arc<arrow_schema::Schema>>`;
- `DatasetSnapshot::new`, `with_schema`, and `resolved_schema`.

The old representation mixed an adapter path and physical Arrow schema into a
stable domain object. PR3 is the authorized migration point because persistence
has not yet shipped to users.

## 3. Dependency boundary

The required direction remains:

```text
stillflow-engine -> stillflow-storage -> stillflow-core
```

PR3 adds `stillflow-storage` to the workspace but does not add an engine call
site. `stillflow-storage` must not depend on `stillflow-plan`,
`stillflow-connectors`, `stillflow-engine`, `stillflow-api`, Polars, DuckDB,
SQLx, Axum, or the `arrow` meta crate.

SQLite, Parquet, filesystem paths, SQL rows, and database connections must not
enter `stillflow-core` public types.

## 4. Authorized files and crates

Expected edits are limited to:

- `backend/Cargo.toml`;
- `backend/Cargo.lock`;
- `backend/crates/stillflow-core/src/domain/snapshot.rs`;
- the minimal `stillflow-core` export and serialization-test files required by
  the snapshot migration;
- `backend/crates/stillflow-storage/Cargo.toml`;
- `backend/crates/stillflow-storage/src/`;
- this contract.

No frontend file, engine behavior, connector behavior, CI definition, or other
architecture document may change. A compile repair outside this list is a
contract stop and must be recorded before implementation continues.

## 5. Dependency changes

The implementation may add only these dependencies:

```toml
parquet = { version = "59", default-features = false, features = ["arrow", "snap"] }
rusqlite = { version = "=0.32.1", features = ["bundled"] }
sha2 = "0.11"
fs2 = "0.4"
tempfile = "3" # stillflow-storage dev-dependency only
```

`parquet` must resolve within Arrow major version 59 already present in the
workspace. `rusqlite` uses bundled SQLite so desktop builds do not silently
depend on a system SQLite version. `sha2` supplies SHA-256 content checksums.
`fs2` supplies a cross-platform advisory root lock. No ORM, migration framework,
async SQLite wrapper, path-capability framework, or checksum helper crate is in
scope.

## 6. Stable core snapshot contract

`stillflow-core` introduces:

```rust
pub const DATASET_SNAPSHOT_VERSION: u16 = 1;

pub struct SnapshotStats {
    row_count: u64,
    stored_byte_count: u64,
    partition_count: u32,
}

pub struct DatasetSnapshot {
    version: u16,
    id: Uuid,
    dataset_id: Uuid,
    session_id: Uuid,
    source_asset_id: Uuid,
    schema: LogicalSchema,
    schema_fingerprint: LogicalSchemaFingerprint,
    stats: SnapshotStats,
    lineage: BTreeSet<Uuid>,
    quality_score: Option<u8>,
    created_at: DateTime<Utc>,
}
```

Field visibility must remain private. Read-only accessors are public.

Construction is fallible and explicit:

- `SnapshotStats::try_new` validates structural count relationships;
- `DatasetSnapshot::try_new` uses version 1;
- `DatasetSnapshot::try_from_parts` accepts a version for migrations and rejects
  unsupported values;
- identity and `created_at` are caller supplied;
- deserialization must use the same validation path as direct construction.

`DatasetSnapshot` must not generate a UUID, read a clock, accept a path, or cache
an Arrow schema.

### 6.1 Core invariants

- `version == DATASET_SNAPSHOT_VERSION`;
- snapshot, dataset, session, source-asset, and lineage UUIDs are non-nil;
- `schema.validate()` succeeds;
- `schema_fingerprint` is recomputed from the complete logical schema;
- `quality_score` is absent or in `0..=100`;
- if `partition_count == 0`, both counts are zero;
- if `partition_count > 0`, row and stored-byte counts are both non-zero;
- lineage serialization is deterministic because `BTreeSet` supplies canonical
  order.

Invalid serialized state must fail closed. Derived `Deserialize` that bypasses
validation is forbidden.

## 7. Storage public surface

The new crate exposes adapter-neutral handles and metadata, not SQL or path
objects:

```rust
pub const STORAGE_SCHEMA_VERSION: u16 = 1;

pub struct StorageLimits { /* private fields and validated accessors */ }
pub struct SnapshotDraft { /* private fields and validated accessors */ }
pub struct ContentDigest([u8; 32]);
pub struct SnapshotPartition { /* sequence, rows, stored bytes, digest */ }
pub struct SnapshotManifest { /* DatasetSnapshot plus ordered partitions */ }
pub struct RecoveryReport { /* examined, recovered, ignored */ }
pub struct GarbageCollectionReport { /* examined, deleted, retained */ }
pub struct SnapshotStore { /* managed-root ownership */ }
pub struct SnapshotWriter { /* one incremental publication */ }
pub struct SnapshotBatchReader { /* lazy Iterator over envelopes */ }
```

Required operations are:

```rust
SnapshotStore::open(root, limits)
SnapshotStore::begin_snapshot(draft, started_at)
SnapshotStore::load_manifest(snapshot_id)
SnapshotStore::read_batches(snapshot_id)
SnapshotStore::verify_snapshot(snapshot_id)
SnapshotStore::tombstone_snapshot(snapshot_id, tombstoned_at)
SnapshotStore::recover(now, stale_after, max_candidates)
SnapshotStore::collect_garbage(now, retention, max_candidates)

SnapshotWriter::append(&BatchEnvelope)
SnapshotWriter::commit()
```

`SnapshotBatchReader` implements
`Iterator<Item = Result<BatchEnvelope, StorageError>>`. It opens and verifies one
partition at a time. A method returning all batches as `Vec` is forbidden.

`SnapshotStore` may be cloned only by sharing the same managed-root lock and
activity state. Independently opening the same root while one store owns it must
fail with a sanitized busy error.

## 8. Storage limits and complexity

Version 1 defaults and hard ceilings are:

| Resource | Bound |
| --- | ---: |
| input envelopes per publication | 16,384 |
| non-empty Parquet partitions per snapshot | 16,384 |
| logical rows per snapshot | 1,000,000,000 |
| stored Parquet bytes per snapshot | 1 TiB |
| recovery candidates per call | 1,024 |
| garbage-collection candidates per call | 1,024 |
| active readers per managed root | 64 |
| active publishers per managed root | 8 |
| SHA-256 read buffer | 64 KiB |
| SQLite busy timeout | 5 seconds |

`StorageLimits` may lower snapshot row, byte, partition, and activity bounds but
must not raise these hard ceilings. Zero limits are invalid.

All additions and integer conversions use checked arithmetic. SQLite integer
values must fit signed 64-bit storage before binding.

For `B` total partition bytes, `P` non-empty partitions, and logical schema size
`S`:

```text
publish time  = O(B + P)
verify time   = O(B + P)
manifest time = O(P)
memory        = O(MAX_BATCH_BYTES + S)
```

The store must not retain prior payload batches, whole Parquet files, or the
entire snapshot in memory. Parquet may buffer only the current bounded envelope
and its row-group encoding state.

## 9. Managed filesystem layout

The local layout is versioned by SQLite, with deterministic internal names:

```text
<managed-root>/
  .stillflow.lock
  metadata.sqlite3
  staging/<snapshot-uuid>/<partition-sequence>.parquet
  partitions/<snapshot-uuid>/<partition-sequence>-<sha256>.parquet
```

User text must never form a path component. Snapshot UUIDs, checked numeric
sequences, and lowercase checksum hex are the only dynamic components.

The SQLite manifest stores checksum and sequence, not a path. The implementation
derives the only valid path from trusted manifest fields. Unknown directory
entries are never recursively deleted.

Managed roots and data entries must be inspected with `symlink_metadata`.
Symlinked roots, staging directories, snapshot directories, or partition files
must fail closed. Error display and debug output must not reveal the root or an
absolute path.

Staging and final snapshot directories are on the same managed filesystem, so
the staging-to-final `rename` is the atomic file publication primitive.

## 10. Parquet partition contract

- Apache Parquet 59 is the only snapshot payload format.
- One non-empty validated input envelope becomes one immutable Parquet file.
- Empty envelopes advance input validation state but create no physical file.
- Stored partition sequences are compact and exactly `0..P` even when input
  contains empty envelopes.
- Writer compression is Snappy.
- A row group contains at most `MAX_BATCH_ROWS` rows.
- The canonical Arrow schema and Stillflow metadata from PR2 are written.
- Files are created with create-new semantics and are never overwritten.
- Each completed file is flushed, finalized, and `sync_all` is called before its
  checksum is accepted.
- SHA-256 covers the complete final Parquet byte sequence and is computed with a
  fixed 64 KiB buffer.
- File byte length comes from filesystem metadata after finalization.

An existing final snapshot directory for the same UUID is an identity conflict,
not an overwrite target.

## 11. Boundary validation and batch-partition law

The storage boundary revalidates every input envelope even if a connector stream
already validated it:

- input sequence begins at zero and increments exactly once, including empty
  envelopes;
- source asset identity equals `SnapshotDraft.source_asset_id`;
- schema fingerprint matches the draft;
- the complete logical schema equals the draft schema after a fingerprint match;
- envelope row and Arrow-memory bounds remain valid through its constructor;
- aggregate limits remain valid before a file is installed.

Batch partitioning is not semantic. For any ordered logical row sequence `R`,
schema `S`, and two accepted envelope partitions `P1` and `P2`:

```text
flatten(read(publish(S, R, P1))) = R
flatten(read(publish(S, R, P2))) = R
```

Snapshot IDs, timestamps, and checksums may differ because they are caller or
physical-format facts. Logical schema and ordered rows must not differ.

## 12. SQLite schema and migration

SQLite `PRAGMA user_version` is the format-version authority. Opening version 0
applies migration 1 transactionally. Opening version 1 is idempotent. Any version
greater than 1 fails closed without mutation.

Every connection must enable:

```text
foreign_keys = ON
journal_mode = WAL
synchronous = FULL
busy_timeout = 5000 ms
```

Schema version 1 contains only these responsibilities:

```text
publications
  snapshot_id primary key
  started_at_utc

snapshots
  snapshot identity and version
  dataset/session/source identity
  canonical logical-schema JSON and fingerprint
  row/stored-byte/partition totals
  canonical lineage JSON
  optional quality score
  created timestamp
  state: visible or tombstoned
  optional tombstone timestamp

partitions
  snapshot_id foreign key
  compact sequence
  row count
  stored byte count
  SHA-256 digest
  primary key (snapshot_id, sequence)
```

Tables must use constraints for non-negative counts, allowed states, checksum
length, and quality range. SQLite values are still revalidated when loaded;
database constraints are defense in depth, not the domain constructor.

General Session, Plan, Job, Event, Dataset, and credential repositories are
explicitly deferred.

## 13. Atomic publication protocol

Publication has one visibility point: the SQLite manifest commit.

1. Validate the draft, timestamp ordering, limits, root ownership, and active
   publisher capacity.
2. Insert a committed `publications` journal row before creating mutable files.
3. Create a private staging directory using the caller-supplied snapshot UUID.
4. For every envelope, validate stream invariants. Skip physical output for an
   empty envelope; otherwise write, finalize, sync, hash, and record one staged
   Parquet partition.
5. On commit, create the final snapshot directory and rename every staged file
   into its checksum-derived immutable final name.
6. Sync final files and directory metadata where the platform supports directory
   synchronization.
7. In one immediate SQLite transaction, insert the visible snapshot row, insert
   every ordered partition row, and delete the publication journal row.
8. Commit SQLite. Only this step makes the snapshot visible.
9. Remove empty staging residue.

Manifest conservation laws are checked immediately before step 7 and again when
loading:

```text
snapshot.row_count             = sum(partition.row_count)
snapshot.stored_byte_count     = sum(partition.stored_byte_count)
snapshot.partition_count       = count(partitions)
partition.sequence             = index(partition)
```

If any step before the SQLite commit fails, no snapshot is visible. Live-process
cleanup is best effort; the journal makes crash cleanup repeatable.

If cleanup after the SQLite commit fails, the complete snapshot remains visible.
Recovery may remove staging residue but must not remove the committed final
directory.

## 14. Read and integrity protocol

`load_manifest` returns only `visible` rows and reconstructs every core domain
type through validated constructors. Tombstoned or unpublished IDs behave as
not found to ordinary readers.

`read_batches` acquires an active-reader slot and holds it until the iterator is
dropped. For each ordered partition it must:

1. derive the final path from snapshot UUID, compact sequence, and checksum;
2. reject symlinks and non-regular files;
3. compare filesystem byte length with the manifest;
4. stream SHA-256 and compare the digest;
5. decode at most one `MAX_BATCH_ROWS` record batch from that partition;
6. reject missing, extra, or row-count-mismatched batches;
7. reconstruct `BatchEnvelope` with compact sequence, source identity, and the
   full manifest logical schema.

Integrity is checked before Parquet values are returned. Errors identify only
snapshot ID, partition sequence, and structural failure category.

`verify_snapshot` performs the same length, checksum, Parquet metadata, row, and
schema checks while discarding decoded batches one partition at a time.

## 15. Activity ownership and concurrency

One process owns a managed root through an exclusive advisory lock held for the
shared lifetime of all `SnapshotStore` clones. A second independent open fails
fast.

Inside the process, shared activity state has three modes:

- up to 64 active readers;
- up to 8 active publishers;
- one exclusive maintenance operation (`recover` or `collect_garbage`).

Acquisition is fail-fast. Maintenance returns `Busy` while any reader or
publisher is active; new readers and publishers return `Busy` during
maintenance. No unbounded wait queue or background task is allowed.

SQLite uniqueness prevents duplicate publication of one snapshot UUID.
Different snapshot publishers may run concurrently within the configured bound.

## 16. Recovery protocol

Recovery is explicit and receives caller-supplied `now`, `stale_after`, and
`max_candidates`. It must not read the ambient clock.

A publication is stale when its normalized UTC start timestamp is not newer than
`now - stale_after`.

For at most `min(max_candidates, 1024)` stale journal rows:

- if no visible snapshot exists, remove only its UUID-derived staging and final
  directories, then remove the journal row;
- if a visible snapshot exists, preserve its final directory and remove only the
  journal/staging residue.

Recovery also scans at most the same bound of UUID-named staging directories.
A staging directory without a journal row is residue and may be removed. Unknown
names and symlinks are ignored and reported; they are never traversed.

Every cleanup step is idempotent. Missing unpublished files are success, not
corruption. Missing files referenced by a visible snapshot remain an integrity
error and are never silently hidden or deleted from the manifest.

## 17. Tombstone and garbage-collection protocol

Deletion is two phase:

1. `tombstone_snapshot` changes a visible snapshot to tombstoned in one SQLite
   transaction using a caller-supplied timestamp not earlier than creation.
2. `collect_garbage` receives caller-supplied `now`, retention duration, and a
   candidate bound. Only tombstones at or before `now - retention` are eligible.

GC requires exclusive maintenance activity. It must never process a visible
snapshot or run while an in-process reader/publisher is active.

For each eligible tombstone, GC removes the exact UUID-derived final directory
first and then removes its SQLite manifest transactionally. A crash between
these operations leaves a tombstone that the next call may safely retry.

Young tombstones, visible snapshots, active snapshots, unknown directory names,
and entries beyond the call bound are retained. Version 1 has no undelete or
restore transition.

## 18. Error and security semantics

`StorageError` must distinguish at least:

- invalid configuration or snapshot state;
- unsupported storage/schema version;
- not found and already exists;
- busy/root already owned;
- sequence, lineage, and schema drift;
- row, byte, partition, envelope, reader, and publisher bounds;
- database, Parquet, serialization, filesystem, and lock operations;
- corrupt manifest and partition integrity failure.

Raw third-party error strings are not public error messages. Filesystem errors
may expose only the operation and `std::io::ErrorKind`; SQLite errors may expose
only a stable operation category/code; Parquet/Arrow errors may expose only the
operation category. Debug output follows the same restriction.

No error, log, manifest, file name, fixture assertion, or SQLite value may
contain:

- record values;
- credentials or connection strings;
- SQL text;
- an absolute managed-root path;
- a user-supplied locator.

Only credential-free logical schema metadata accepted by `LogicalSchema` may be
persisted.

## 19. Cancellation and time semantics

Version 1 is a synchronous, partition-granular storage adapter. It does not
introduce an async runtime or a second cancellation-token contract.

- A caller cancels publication between bounded `append` calls by dropping
  `SnapshotWriter`; drop performs best-effort abort and recovery remains the
  crash-safe fallback.
- One append is bounded by the existing 64 MiB Arrow envelope and one Parquet
  partition.
- A reader may stop between partitions by dropping the iterator.
- Recovery and GC are bounded by caller-supplied candidate counts.
- SQLite waits at most five seconds for a busy lock.
- Identity, creation, staleness, tombstoning, and retention never consult an
  ambient clock.

Engine orchestration may later place these blocking operations on a bounded
blocking pool. Adding Tokio tasks, channels, or background cleanup in PR3 is
forbidden.

## 20. Ordered implementation checklist

1. Add the Issue #28 contract as the first branch commit.
2. Replace `DatasetSnapshot` and remove the physical schema snapshot encoder.
3. Add validated snapshot serialization and negative tests in core.
4. Add the workspace/storage manifests and regenerate the committed lockfile.
5. Add sanitized errors, limits, SHA-256 digest, manifest metadata, and activity
   accounting.
6. Add root initialization, advisory locking, SQLite connection policy, and
   migration 1.
7. Add publication journaling and incremental Parquet partition writes.
8. Add atomic install and SQLite visibility commit.
9. Add manifest loading, lazy reads, and integrity verification.
10. Add explicit bounded recovery.
11. Add tombstoning and bounded garbage collection.
12. Add failure-point, corruption, concurrency, invariance, and bounds tests.
13. Run final architecture and contract review before changing Draft status.

## 21. Acceptance tests

### Core contract

- version 1 snapshot JSON round-trips deterministically;
- unsupported versions, nil identities, nil lineage, invalid logical schemas,
  invalid quality scores, fingerprint mismatch, and contradictory totals fail;
- physical Arrow schemas and storage paths are absent from serialized snapshots;
- two equivalent logical schemas produce the same stored fingerprint.

### Migration and ownership

- a new root migrates atomically to SQLite version 1;
- reopening version 1 is idempotent and preserves data;
- a future `user_version` fails without mutation;
- a second independent store open fails while the first root lock is held and
  succeeds after release;
- configured concurrency and resource limits reject overflow.

### Publication and read

- a snapshot remains not found before manifest commit;
- publish/load/read preserves schema, column IDs, source identity, lineage,
  ordered rows, timestamps, quality score, and exact totals;
- duplicate snapshot IDs fail without overwrite;
- sequence gaps/duplicates, lineage changes, fingerprint changes, full-schema
  changes, row overflow, stored-byte overflow, partition overflow, and envelope
  overflow fail;
- empty snapshots publish with zero files and zero totals;
- empty input envelopes create no empty partition;
- alternate valid batch partitions reconstruct identical ordered rows;
- reading is lazy and stops without opening later partitions after iterator drop.

### Integrity and crash behavior

- pre-commit installed files are invisible and stale recovery removes them;
- post-commit staging residue is removed while the visible final snapshot is
  preserved;
- missing files, modified length, checksum mismatch, invalid Parquet, schema
  mismatch, row mismatch, and symlinked entries fail closed;
- errors and debug output omit payload values and absolute root paths.

### Tombstone and GC

- tombstoning immediately hides a snapshot from ordinary readers;
- visible and young tombstoned snapshots are not collected;
- eligible old tombstones are removed within the candidate bound;
- missing files for a tombstone are retried safely;
- GC returns busy while an active reader or publisher exists;
- repeated recovery and GC converge without changing visible snapshots.

### Repository gates

- `cargo fmt --all -- --check` passes;
- `cargo clippy --workspace --all-targets -- -D warnings` passes;
- `cargo test --workspace` passes;
- `npm run typecheck` passes;
- `npm run build` passes;
- no frontend source, CSS, layout, token, or dependency file changes;
- no production `unwrap` or `expect` is added;
- no whole-snapshot `collect` or unbounded task/channel is added.

## 22. Known risks

- Parquet compression can require a constant-factor encoding buffer in addition
  to the bounded Arrow batch; the asymptotic memory bound remains one batch.
- Directory `fsync` support differs by operating system. File `sync_all`,
  same-filesystem atomic rename, SQLite `synchronous=FULL`, recovery journals,
  and repeatable cleanup remain mandatory; unsupported directory sync is a
  recorded platform limitation rather than a false success.
- SHA-256 verifies bytes, not semantic equivalence. Logical schema and row counts
  are checked separately.
- Version 1 intentionally serializes all operations for one managed root across
  processes through an exclusive lock. Multi-process leases are deferred.
- SQLite and filesystem durability cannot form one distributed transaction.
  The protocol orders durable files before metadata visibility and uses recovery
  to make the unavoidable gap safe.

## 23. Stop conditions

Stop and return to contract review if implementation requires:

- a dependency outside section 5;
- a public path, SQL, SQLite, or Parquet type in core;
- a dependency on connectors, plan, engine, API, Polars, DuckDB, or SQLx;
- overwrite or mutation of a committed Parquet partition;
- a visible manifest before all partition files are durable;
- deletion of an unknown path or visible snapshot;
- an unbounded batch, file read, collection, queue, or background task;
- ambient UUID or timestamp generation;
- payload values, secrets, locators, SQL, or absolute paths in diagnostics;
- frontend behavior or historical-branch code.
