use std::collections::BTreeSet;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::RecordBatch;
use chrono::{DateTime, SecondsFormat, Utc};
use fs2::FileExt;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use uuid::Uuid;

use stillflow_core::{
    logical_schema_to_arrow, BatchEnvelope, DatasetSnapshot, LogicalSchema,
    LogicalSchemaFingerprint, SnapshotStats, DATASET_SNAPSHOT_VERSION, MAX_BATCH_ROWS,
};

use crate::artifact::{
    accepted_partition_canonical_digest, accepted_snapshot_manifest_digest, canonical_batch_bytes,
    AcceptedCanonicalPartition,
};
use crate::digest::digest_file;
use crate::manifest::build_snapshot;
use crate::{
    ContentDigest, GarbageCollectionReport, IntegrityFailure, RecoveryReport, SnapshotDraft,
    SnapshotManifest, SnapshotPartition, StorageError, StorageLimits, MAX_MAINTENANCE_CANDIDATES,
    SQLITE_BUSY_TIMEOUT_MILLIS, STORAGE_SCHEMA_VERSION,
};

const VISIBLE_STATE: i64 = 1;
const TOMBSTONED_STATE: i64 = 2;

#[derive(Clone)]
pub struct SnapshotStore {
    pub(crate) inner: Arc<StoreInner>,
}

impl fmt::Debug for SnapshotStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SnapshotStore")
            .field("storage_schema_version", &STORAGE_SCHEMA_VERSION)
            .field("limits", &self.inner.limits)
            .finish_non_exhaustive()
    }
}

pub(crate) struct StoreInner {
    pub(crate) root: PathBuf,
    pub(crate) limits: StorageLimits,
    _root_lock: File,
    pub(crate) activity: Mutex<ActivityState>,
    /// Live export staging bytes across concurrent exports for this store
    /// root (ADR-004 §5 `MAX_EXPORT_TEMP_BYTES`).
    pub(crate) export_staging_bytes: std::sync::atomic::AtomicU64,
}

#[derive(Debug, Default)]
pub(crate) struct ActivityState {
    readers: u16,
    publishers: u16,
    export_publishers: u16,
    maintenance: bool,
}

#[derive(Clone, Copy)]
pub(crate) enum ActivityKind {
    Reader,
    Publisher,
    ExportPublisher,
}

pub(crate) struct ActivityGuard {
    inner: Arc<StoreInner>,
    kind: ActivityKind,
    active: bool,
}

impl Drop for ActivityGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        if let Ok(mut state) = self.inner.activity.lock() {
            match self.kind {
                ActivityKind::Reader if state.readers > 0 => state.readers -= 1,
                ActivityKind::Publisher if state.publishers > 0 => state.publishers -= 1,
                ActivityKind::ExportPublisher if state.export_publishers > 0 => {
                    state.export_publishers -= 1;
                }
                ActivityKind::Reader | ActivityKind::Publisher | ActivityKind::ExportPublisher => {}
            }
        }
        self.active = false;
    }
}

pub(crate) struct MaintenanceGuard {
    inner: Arc<StoreInner>,
    active: bool,
}

impl Drop for MaintenanceGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        if let Ok(mut state) = self.inner.activity.lock() {
            state.maintenance = false;
        }
        self.active = false;
    }
}

impl SnapshotStore {
    pub fn open(root: impl AsRef<Path>, limits: StorageLimits) -> Result<Self, StorageError> {
        let root = prepare_root(root.as_ref())?;
        let lock_path = root.join(".stillflow.lock");
        reject_symlink_if_present(&lock_path, "inspect managed-root lock")?;
        let root_lock = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|error| StorageError::io("open managed-root lock", &error))?;
        if let Err(error) = FileExt::try_lock_exclusive(&root_lock) {
            if error.kind() == std::io::ErrorKind::WouldBlock {
                return Err(StorageError::Busy("managed root is already owned"));
            }
            return Err(StorageError::io("acquire managed-root lock", &error));
        }

        let inner = Arc::new(StoreInner {
            root,
            limits,
            _root_lock: root_lock,
            activity: Mutex::new(ActivityState::default()),
            export_staging_bytes: std::sync::atomic::AtomicU64::new(0),
        });
        let mut connection = open_connection(&inner)?;
        migrate(&mut connection)?;
        Ok(Self { inner })
    }

    pub fn limits(&self) -> StorageLimits {
        self.inner.limits
    }

    /// Returns the E5 control-plane persistence view sharing this store's
    /// managed-root lock and SQLite schema.
    pub fn control_plane(&self) -> crate::ControlPlaneStore {
        crate::ControlPlaneStore::from_snapshot_store(self)
    }

    pub fn begin_snapshot(
        &self,
        draft: SnapshotDraft,
        started_at: DateTime<Utc>,
    ) -> Result<SnapshotWriter, StorageError> {
        if draft.created_at() > &started_at {
            return Err(StorageError::InvalidTimestampOrder(
                "snapshot creation and publication start",
            ));
        }
        let activity = acquire_activity(&self.inner, ActivityKind::Publisher)?;
        insert_publication(&self.inner, draft.id(), &started_at)?;

        let staging_dir = staging_snapshot_dir(&self.inner, draft.id());
        match create_exact_directory(&staging_dir, "create snapshot staging directory") {
            Ok(()) => {}
            Err(error) => {
                abort_publication(&self.inner, draft.id());
                return Err(error);
            }
        }

        Ok(SnapshotWriter {
            inner: Arc::clone(&self.inner),
            _activity: Some(activity),
            draft,
            staging_dir,
            staged: Vec::new(),
            next_input_sequence: 0,
            envelope_count: 0,
            row_count: 0,
            stored_byte_count: 0,
            installed: false,
            committed: false,
            failed: false,
        })
    }

    pub fn load_manifest(&self, snapshot_id: Uuid) -> Result<SnapshotManifest, StorageError> {
        let _activity = acquire_activity(&self.inner, ActivityKind::Reader)?;
        load_manifest_inner(&self.inner, snapshot_id)
    }

    pub fn read_batches(&self, snapshot_id: Uuid) -> Result<SnapshotBatchReader, StorageError> {
        let activity = acquire_activity(&self.inner, ActivityKind::Reader)?;
        let manifest = load_manifest_inner(&self.inner, snapshot_id)?;
        Ok(SnapshotBatchReader {
            inner: Arc::clone(&self.inner),
            _activity: activity,
            manifest,
            next_partition: 0,
        })
    }

    /// Returns the logical committed version digest used by E5 typed
    /// Snapshot references. The digest is derived from the immutable Snapshot
    /// descriptor and canonical logical batch bytes, never from Parquet file
    /// compression, paths, or mutable filesystem metadata.
    pub fn version_digest(&self, snapshot_id: Uuid) -> Result<[u8; 32], StorageError> {
        let _activity = acquire_activity(&self.inner, ActivityKind::Reader)?;
        snapshot_version_digest_inner(&self.inner, snapshot_id)
    }

    pub fn verify_snapshot(&self, snapshot_id: Uuid) -> Result<SnapshotManifest, StorageError> {
        let activity = acquire_activity(&self.inner, ActivityKind::Reader)?;
        let manifest = load_manifest_inner(&self.inner, snapshot_id)?;
        for partition in manifest.partitions() {
            read_partition(&self.inner, manifest.snapshot(), partition)?;
        }
        drop(activity);
        Ok(manifest)
    }

    pub fn tombstone_snapshot(
        &self,
        snapshot_id: Uuid,
        tombstoned_at: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        let _activity = acquire_activity(&self.inner, ActivityKind::Reader)?;
        let mut connection = open_connection(&self.inner)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StorageError::database("begin tombstone transaction"))?;
        let created_at: Option<String> = transaction
            .query_row(
                "SELECT created_at_utc FROM snapshots WHERE id = ?1 AND state = ?2",
                params![snapshot_id.to_string(), VISIBLE_STATE],
                |row| row.get(0),
            )
            .optional()
            .map_err(|_| StorageError::database("read snapshot tombstone state"))?;
        let Some(created_at) = created_at else {
            return Err(StorageError::NotFound(snapshot_id));
        };
        let created_at = parse_timestamp(&created_at, "snapshot creation timestamp")?;
        if tombstoned_at < created_at {
            return Err(StorageError::InvalidTimestampOrder(
                "snapshot creation and tombstone",
            ));
        }
        let updated = transaction
            .execute(
                "UPDATE snapshots
                 SET state = ?2, tombstoned_at_utc = ?3
                 WHERE id = ?1 AND state = ?4",
                params![
                    snapshot_id.to_string(),
                    TOMBSTONED_STATE,
                    format_timestamp(&tombstoned_at),
                    VISIBLE_STATE
                ],
            )
            .map_err(|_| StorageError::database("tombstone snapshot"))?;
        if updated != 1 {
            return Err(StorageError::NotFound(snapshot_id));
        }
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit tombstone transaction"))
    }

    pub fn recover(
        &self,
        now: DateTime<Utc>,
        stale_after: Duration,
        max_candidates: u32,
    ) -> Result<RecoveryReport, StorageError> {
        validate_maintenance_bound(max_candidates)?;
        let _maintenance = acquire_maintenance(&self.inner)?;
        let cutoff = cutoff_timestamp(now, stale_after, "recovery cutoff")?;
        let candidates = stale_publications(&self.inner, &cutoff, max_candidates)?;
        let mut report = RecoveryReport::default();

        for snapshot_id in candidates {
            checked_increment(&mut report.examined, "recovery examined count")?;
            if snapshot_is_visible(&self.inner, snapshot_id)? {
                let outcome = remove_uuid_directory(
                    &staging_root(&self.inner),
                    snapshot_id,
                    SymlinkPolicy::Ignore,
                    "remove committed staging residue",
                )?;
                if outcome == RemovalOutcome::Ignored {
                    checked_increment(&mut report.ignored, "recovery ignored count")?;
                }
            } else {
                for root in [staging_root(&self.inner), partitions_root(&self.inner)] {
                    let outcome = remove_uuid_directory(
                        &root,
                        snapshot_id,
                        SymlinkPolicy::Ignore,
                        "remove unpublished snapshot residue",
                    )?;
                    if outcome == RemovalOutcome::Ignored {
                        checked_increment(&mut report.ignored, "recovery ignored count")?;
                    }
                }
            }
            delete_publication(&self.inner, snapshot_id)?;
            checked_increment(&mut report.recovered, "recovery recovered count")?;
        }

        recover_bundles(&self.inner, &cutoff, max_candidates, &mut report)?;
        scan_orphan_staging(&self.inner, max_candidates, &mut report)?;
        crate::export::recover_export_residue(&self.inner, &cutoff, max_candidates, &mut report)?;
        crate::dedup::recover_dedup_candidates(&self.inner, max_candidates, &mut report)?;
        Ok(report)
    }

    pub fn collect_garbage(
        &self,
        now: DateTime<Utc>,
        retention: Duration,
        max_candidates: u32,
    ) -> Result<GarbageCollectionReport, StorageError> {
        validate_maintenance_bound(max_candidates)?;
        let _maintenance = acquire_maintenance(&self.inner)?;
        let cutoff = cutoff_timestamp(now, retention, "garbage-collection cutoff")?;
        let candidates = eligible_tombstones(&self.inner, &cutoff, max_candidates)?;
        let mut report = GarbageCollectionReport::default();

        for snapshot_id in candidates {
            checked_increment(&mut report.examined, "garbage-collection examined count")?;
            let outcome = remove_uuid_directory(
                &partitions_root(&self.inner),
                snapshot_id,
                SymlinkPolicy::Reject,
                "remove tombstoned snapshot files",
            )?;
            if outcome == RemovalOutcome::Ignored {
                checked_increment(&mut report.retained, "garbage-collection retained count")?;
                continue;
            }

            let mut connection = open_connection(&self.inner)?;
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|_| StorageError::database("begin garbage-collection transaction"))?;
            let deleted = transaction
                .execute(
                    "DELETE FROM snapshots
                     WHERE id = ?1 AND state = ?2 AND tombstoned_at_utc <= ?3",
                    params![snapshot_id.to_string(), TOMBSTONED_STATE, cutoff],
                )
                .map_err(|_| StorageError::database("delete tombstoned manifest"))?;
            transaction
                .commit()
                .map_err(|_| StorageError::database("commit garbage-collection transaction"))?;
            if deleted == 1 {
                checked_increment(&mut report.deleted, "garbage-collection deleted count")?;
            } else {
                checked_increment(&mut report.retained, "garbage-collection retained count")?;
            }
        }

        crate::export::collect_export_garbage(&self.inner, &cutoff, max_candidates, &mut report)?;
        Ok(report)
    }

    /// Loads the committed Export Manifest of one visible export (ADR-004
    /// §7). Tombstoned, never-committed, and unknown exports fail typed; the
    /// manifest is revalidated against its own file list on every load so the
    /// digest bookkeeping stays mechanically recomputable.
    pub fn load_export_manifest(
        &self,
        export_id: Uuid,
    ) -> Result<crate::export::ExportManifest, StorageError> {
        let _activity = acquire_activity(&self.inner, ActivityKind::Reader)?;
        crate::export::load_export_manifest_inner(&self.inner, export_id)
    }

    /// Tombstones one committed export: the manifest stops being visible to
    /// ordinary reads while its bytes stay recoverable until an explicit
    /// retention cutoff collects them (ADR-004 §7 tombstone-first deletion).
    pub fn tombstone_export(
        &self,
        export_id: Uuid,
        tombstoned_at: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        let _activity = acquire_activity(&self.inner, ActivityKind::Reader)?;
        crate::export::tombstone_export_inner(&self.inner, export_id, &tombstoned_at)
    }
}

/// Computes the committed logical Snapshot version digest without opening a
/// second activity guard. Callers either hold the reader guard themselves or
/// are inside a control-plane transaction that already serializes the
/// publication decision.
pub(crate) fn snapshot_version_digest_inner(
    inner: &StoreInner,
    snapshot_id: Uuid,
) -> Result<[u8; 32], StorageError> {
    let manifest = load_manifest_inner(inner, snapshot_id)?;
    let mut canonical_partitions = Vec::with_capacity(manifest.partitions().len());
    for partition in manifest.partitions() {
        let envelope = read_partition(inner, manifest.snapshot(), partition)?;
        if envelope.row_count() as u64 != partition.row_count() {
            return Err(StorageError::InvalidManifest(
                "Snapshot partition row count differs from its logical batch",
            ));
        }
        let canonical = canonical_batch_bytes(envelope.payload())?;
        let stored_byte_count = u64::try_from(canonical.len())
            .map_err(|_| StorageError::ArithmeticOverflow("Snapshot logical byte count"))?;
        let digest = accepted_partition_canonical_digest(
            manifest.snapshot().id(),
            partition.sequence(),
            partition.row_count(),
            stored_byte_count,
            &[canonical],
        );
        canonical_partitions.push(AcceptedCanonicalPartition {
            sequence: partition.sequence(),
            row_count: partition.row_count(),
            stored_byte_count,
            digest,
        });
    }
    accepted_snapshot_manifest_digest(manifest.snapshot(), &canonical_partitions)
}

pub struct SnapshotWriter {
    inner: Arc<StoreInner>,
    _activity: Option<ActivityGuard>,
    draft: SnapshotDraft,
    staging_dir: PathBuf,
    staged: Vec<SnapshotPartition>,
    next_input_sequence: u64,
    envelope_count: u32,
    row_count: u64,
    stored_byte_count: u64,
    installed: bool,
    committed: bool,
    failed: bool,
}

impl SnapshotWriter {
    pub fn append(&mut self, envelope: &BatchEnvelope) -> Result<(), StorageError> {
        if self.failed {
            return Err(StorageError::InvalidDraft(
                "snapshot writer is already in a failed state",
            ));
        }
        let result = self.append_inner(envelope);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn append_inner(&mut self, envelope: &BatchEnvelope) -> Result<(), StorageError> {
        let envelope_count = self
            .envelope_count
            .checked_add(1)
            .ok_or(StorageError::ArithmeticOverflow("input envelope count"))?;
        if envelope_count > self.inner.limits.max_input_envelopes() {
            return Err(StorageError::EnvelopeLimitExceeded {
                actual: envelope_count,
                maximum: self.inner.limits.max_input_envelopes(),
            });
        }
        if envelope.sequence() != self.next_input_sequence {
            return Err(StorageError::Sequence {
                expected: self.next_input_sequence,
                actual: envelope.sequence(),
            });
        }
        if envelope.source_asset_id() != self.draft.source_asset_id() {
            return Err(StorageError::LineageMismatch {
                sequence: envelope.sequence(),
            });
        }
        if envelope.schema_fingerprint() != self.draft.schema_fingerprint()
            || envelope.schema() != self.draft.schema()
        {
            return Err(StorageError::SchemaDrift {
                sequence: envelope.sequence(),
            });
        }

        self.next_input_sequence = self
            .next_input_sequence
            .checked_add(1)
            .ok_or(StorageError::ArithmeticOverflow("input sequence"))?;
        self.envelope_count = envelope_count;
        if envelope.row_count() == 0 {
            return Ok(());
        }

        let partition_sequence = u32::try_from(self.staged.len())
            .map_err(|_| StorageError::ArithmeticOverflow("partition sequence"))?;
        let partition_count = partition_sequence
            .checked_add(1)
            .ok_or(StorageError::ArithmeticOverflow("partition count"))?;
        if partition_count > self.inner.limits.max_partitions() {
            return Err(StorageError::PartitionLimitExceeded {
                actual: partition_count,
                maximum: self.inner.limits.max_partitions(),
            });
        }

        let envelope_rows = u64::try_from(envelope.row_count())
            .map_err(|_| StorageError::ArithmeticOverflow("envelope row count"))?;
        let row_count = self
            .row_count
            .checked_add(envelope_rows)
            .ok_or(StorageError::ArithmeticOverflow("snapshot row count"))?;
        if row_count > self.inner.limits.max_rows() {
            return Err(StorageError::RowLimitExceeded {
                actual: row_count,
                maximum: self.inner.limits.max_rows(),
            });
        }

        let partition = write_partition(&self.staging_dir, partition_sequence, envelope)?;
        let stored_byte_count = self
            .stored_byte_count
            .checked_add(partition.stored_byte_count())
            .ok_or(StorageError::ArithmeticOverflow(
                "snapshot stored byte count",
            ))?;
        if stored_byte_count > self.inner.limits.max_stored_bytes() {
            remove_staged_partition(&self.staging_dir, partition_sequence);
            return Err(StorageError::StoredByteLimitExceeded {
                actual: stored_byte_count,
                maximum: self.inner.limits.max_stored_bytes(),
            });
        }

        self.staged.push(partition);
        self.row_count = row_count;
        self.stored_byte_count = stored_byte_count;
        Ok(())
    }

    pub fn commit(mut self) -> Result<SnapshotManifest, StorageError> {
        if self.failed {
            return Err(StorageError::InvalidDraft(
                "snapshot writer cannot commit after a failed append",
            ));
        }
        let partition_count = u32::try_from(self.staged.len())
            .map_err(|_| StorageError::ArithmeticOverflow("partition count"))?;
        let stats =
            SnapshotStats::try_new(self.row_count, self.stored_byte_count, partition_count)?;
        let snapshot = build_snapshot(&self.draft, stats)?;
        let manifest = SnapshotManifest::try_new(snapshot, self.staged.clone())?;

        create_final_snapshot_directory(&self.inner, self.draft.id())?;
        self.installed = true;
        install_partitions(&self.inner, self.draft.id(), &manifest)?;
        commit_manifest(&self.inner, &manifest)?;
        self.committed = true;

        let _ = remove_uuid_directory(
            &staging_root(&self.inner),
            self.draft.id(),
            SymlinkPolicy::Ignore,
            "remove committed staging directory",
        );
        Ok(manifest)
    }
}

impl Drop for SnapshotWriter {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let _ = remove_uuid_directory(
            &staging_root(&self.inner),
            self.draft.id(),
            SymlinkPolicy::Ignore,
            "abort snapshot staging directory",
        );
        if self.installed {
            let _ = remove_uuid_directory(
                &partitions_root(&self.inner),
                self.draft.id(),
                SymlinkPolicy::Ignore,
                "abort installed snapshot directory",
            );
        }
        abort_publication(&self.inner, self.draft.id());
    }
}

pub struct SnapshotBatchReader {
    inner: Arc<StoreInner>,
    _activity: ActivityGuard,
    manifest: SnapshotManifest,
    next_partition: usize,
}

impl SnapshotBatchReader {
    pub fn manifest(&self) -> &SnapshotManifest {
        &self.manifest
    }
}

impl Iterator for SnapshotBatchReader {
    type Item = Result<BatchEnvelope, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        let partition = self.manifest.partitions().get(self.next_partition)?.clone();
        self.next_partition += 1;
        Some(read_partition(
            &self.inner,
            self.manifest.snapshot(),
            &partition,
        ))
    }
}

fn prepare_root(root: &Path) -> Result<PathBuf, StorageError> {
    match fs::symlink_metadata(root) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(StorageError::InvalidConfiguration(
                    "managed root must be a non-symlink directory",
                ));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(root)
                .map_err(|error| StorageError::io("create managed root", &error))?;
        }
        Err(error) => return Err(StorageError::io("inspect managed root", &error)),
    }
    let root = fs::canonicalize(root)
        .map_err(|error| StorageError::io("canonicalize managed root", &error))?;
    ensure_managed_directory(&root.join("staging"), "prepare staging root")?;
    ensure_managed_directory(&root.join("partitions"), "prepare partitions root")?;
    ensure_managed_directory(&root.join("export-staging"), "prepare export staging root")?;
    ensure_private_directory(&root.join("temp"))?;
    Ok(root)
}

/// Creates a `0700` (Unix) managed directory for owner-only temp state.
pub(crate) fn ensure_private_directory(path: &Path) -> Result<(), StorageError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => Err(
            StorageError::InvalidConfiguration("managed entry must be a non-symlink directory"),
        ),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(path).map_err(|error| StorageError::io("create temp root", &error))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(path, fs::Permissions::from_mode(0o700))
                    .map_err(|error| StorageError::io("restrict temp root permissions", &error))?;
            }
            Ok(())
        }
        Err(error) => Err(StorageError::io("inspect temp root", &error)),
    }
}

pub(crate) fn ensure_managed_directory(
    path: &Path,
    operation: &'static str,
) -> Result<(), StorageError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => Err(
            StorageError::InvalidConfiguration("managed entry must be a non-symlink directory"),
        ),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(path).map_err(|error| StorageError::io(operation, &error))
        }
        Err(error) => Err(StorageError::io(operation, &error)),
    }
}

fn reject_symlink_if_present(path: &Path, operation: &'static str) -> Result<(), StorageError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(
            StorageError::InvalidConfiguration("managed entry must not be a symlink"),
        ),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(StorageError::io(operation, &error)),
    }
}

pub(crate) fn create_exact_directory(
    path: &Path,
    operation: &'static str,
) -> Result<(), StorageError> {
    fs::create_dir(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::AlreadyExists {
            StorageError::InvalidConfiguration("managed snapshot directory already exists")
        } else {
            StorageError::io(operation, &error)
        }
    })
}

pub(crate) fn staging_root(inner: &StoreInner) -> PathBuf {
    inner.root.join("staging")
}

pub(crate) fn export_staging_root(inner: &StoreInner) -> PathBuf {
    inner.root.join("export-staging")
}

pub(crate) fn partitions_root(inner: &StoreInner) -> PathBuf {
    inner.root.join("partitions")
}

fn staging_snapshot_dir(inner: &StoreInner, snapshot_id: Uuid) -> PathBuf {
    staging_root(inner).join(snapshot_id.to_string())
}

fn final_snapshot_dir(inner: &StoreInner, snapshot_id: Uuid) -> PathBuf {
    partitions_root(inner).join(snapshot_id.to_string())
}

fn staged_partition_path(directory: &Path, sequence: u32) -> PathBuf {
    directory.join(format!("{sequence:010}.parquet"))
}

fn final_partition_path(
    inner: &StoreInner,
    snapshot_id: Uuid,
    partition: &SnapshotPartition,
) -> PathBuf {
    final_snapshot_dir(inner, snapshot_id).join(format!(
        "{:010}-{}.parquet",
        partition.sequence(),
        partition.digest()
    ))
}

pub(crate) fn open_connection(inner: &StoreInner) -> Result<Connection, StorageError> {
    let database_path = inner.root.join("metadata.sqlite3");
    reject_symlink_if_present(&database_path, "inspect metadata database")?;
    let connection = Connection::open(&database_path)
        .map_err(|_| StorageError::database("open metadata database"))?;
    connection
        .busy_timeout(Duration::from_millis(SQLITE_BUSY_TIMEOUT_MILLIS))
        .map_err(|_| StorageError::database("configure SQLite busy timeout"))?;
    connection
        .execute_batch(
            "PRAGMA foreign_keys = ON;
             PRAGMA journal_mode = WAL;
             PRAGMA synchronous = FULL;",
        )
        .map_err(|_| StorageError::database("configure SQLite connection"))?;
    Ok(connection)
}

fn migrate(connection: &mut Connection) -> Result<(), StorageError> {
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(|_| StorageError::database("read storage schema version"))?;
    match version {
        0 => {
            migrate_to_version_one(connection)?;
            migrate_to_version_two(connection)?;
            migrate_to_version_three(connection)?;
            migrate_to_version_four(connection)?;
            migrate_to_version_five(connection)?;
            migrate_to_version_six(connection)?;
            migrate_to_version_seven(connection)
        }
        1 => {
            migrate_to_version_two(connection)?;
            migrate_to_version_three(connection)?;
            migrate_to_version_four(connection)?;
            migrate_to_version_five(connection)?;
            migrate_to_version_six(connection)?;
            migrate_to_version_seven(connection)
        }
        2 => {
            migrate_to_version_three(connection)?;
            migrate_to_version_four(connection)?;
            migrate_to_version_five(connection)?;
            migrate_to_version_six(connection)?;
            migrate_to_version_seven(connection)
        }
        3 => {
            migrate_to_version_four(connection)?;
            migrate_to_version_five(connection)?;
            migrate_to_version_six(connection)?;
            migrate_to_version_seven(connection)
        }
        4 => {
            migrate_to_version_five(connection)?;
            migrate_to_version_six(connection)?;
            migrate_to_version_seven(connection)
        }
        5 => {
            migrate_to_version_six(connection)?;
            migrate_to_version_seven(connection)
        }
        6 => migrate_to_version_seven(connection),
        7 => Ok(()),
        unsupported => Err(StorageError::UnsupportedStorageVersion(unsupported)),
    }
}

/// Version six adds Dataset-owned Q-D1 ProfileHistory references. The table
/// stores no profile payload; it references the committed E5 Artifact body.
fn migrate_to_version_six(connection: &mut Connection) -> Result<(), StorageError> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|_| StorageError::database("begin storage migration version six"))?;
    transaction
        .execute_batch(
            "CREATE TABLE qd1_profile_history (
                 history_id TEXT PRIMARY KEY NOT NULL,
                 workspace_id TEXT NOT NULL REFERENCES cp_workspaces(id),
                 dataset_id TEXT NOT NULL REFERENCES cp_datasets(id),
                 profile_artifact_id TEXT NOT NULL REFERENCES cp_artifact_refs(id),
                 producing_run_id TEXT NOT NULL REFERENCES cp_runs(id),
                 profile_digest TEXT NOT NULL CHECK (length(profile_digest) = 64),
                 profile_contract_version INTEGER NOT NULL CHECK (profile_contract_version > 0),
                 drift_contract_version INTEGER NOT NULL CHECK (drift_contract_version > 0),
                 profile_policy_version INTEGER NOT NULL CHECK (profile_policy_version > 0),
                 top_k INTEGER NOT NULL CHECK (top_k > 0),
                 histogram_buckets INTEGER NOT NULL CHECK (histogram_buckets > 0),
                 schema_fingerprint TEXT NOT NULL CHECK (length(schema_fingerprint) = 64),
                 schema_json TEXT NOT NULL,
                 row_count_scanned INTEGER NOT NULL CHECK (row_count_scanned >= 0),
                 scanned_bytes INTEGER NOT NULL CHECK (scanned_bytes >= 0),
                 truncated INTEGER NOT NULL CHECK (truncated IN (0, 1)),
                 profile_sequence INTEGER NOT NULL CHECK (profile_sequence > 0),
                 state TEXT NOT NULL CHECK (state IN ('active', 'tombstoned')),
                 created_at_utc TEXT NOT NULL,
                 tombstoned_at_utc TEXT,
                 UNIQUE (workspace_id, dataset_id, profile_artifact_id, producing_run_id),
                 UNIQUE (workspace_id, dataset_id, profile_sequence)
             ) STRICT;

             CREATE INDEX qd1_profile_history_order_index
             ON qd1_profile_history(workspace_id, dataset_id, state,
                                     profile_sequence DESC, history_id DESC);

             CREATE INDEX qd1_profile_history_artifact_index
             ON qd1_profile_history(profile_artifact_id, state);

             PRAGMA user_version = 6;",
        )
        .map_err(|_| StorageError::database("apply storage migration version six"))?;
    transaction
        .commit()
        .map_err(|_| StorageError::database("commit storage migration version six"))
}

/// Version seven adds the resolved-comparison identity. The report Artifact
/// is still owned by the existing E5 Run; this row only makes retries return
/// the first committed report for one comparison key.
fn migrate_to_version_seven(connection: &mut Connection) -> Result<(), StorageError> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|_| StorageError::database("begin storage migration version seven"))?;
    transaction
        .execute_batch(
            "CREATE TABLE qd1_drift_comparisons (
                 comparison_key TEXT PRIMARY KEY NOT NULL CHECK (length(comparison_key) = 64),
                 workspace_id TEXT NOT NULL REFERENCES cp_workspaces(id),
                 dataset_id TEXT NOT NULL REFERENCES cp_datasets(id),
                 baseline_history_id TEXT NOT NULL REFERENCES qd1_profile_history(history_id),
                 candidate_history_id TEXT NOT NULL REFERENCES qd1_profile_history(history_id),
                 report_artifact_id TEXT NOT NULL UNIQUE REFERENCES cp_artifact_refs(id),
                 producing_run_id TEXT NOT NULL REFERENCES cp_runs(id),
                 report_digest TEXT NOT NULL CHECK (length(report_digest) = 64),
                 created_at_utc TEXT NOT NULL
             ) STRICT;

             CREATE INDEX qd1_drift_comparisons_scope_index
             ON qd1_drift_comparisons(workspace_id, dataset_id, created_at_utc,
                                      comparison_key);

             PRAGMA user_version = 7;",
        )
        .map_err(|_| StorageError::database("apply storage migration version seven"))?;
    transaction
        .commit()
        .map_err(|_| StorageError::database("commit storage migration version seven"))
}

/// Version four adds the durable E5 unified control-plane graph.  The tables
/// are additive: the existing snapshot, verification-bundle, and export
/// tables are intentionally left untouched and remain owned by their writers.
fn migrate_to_version_four(connection: &mut Connection) -> Result<(), StorageError> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|_| StorageError::database("begin storage migration version four"))?;
    transaction
        .execute_batch(
            "CREATE TABLE cp_workspaces (
                 id TEXT PRIMARY KEY NOT NULL,
                 state TEXT NOT NULL CHECK (state IN ('active', 'archived')),
                 created_at_utc TEXT NOT NULL,
                 archived_at_utc TEXT
             ) STRICT;

             CREATE TABLE cp_sessions (
                 id TEXT PRIMARY KEY NOT NULL,
                 workspace_id TEXT NOT NULL REFERENCES cp_workspaces(id),
                 state TEXT NOT NULL CHECK (state IN ('open', 'closing', 'closed')),
                 created_at_utc TEXT NOT NULL,
                 updated_at_utc TEXT NOT NULL
             ) STRICT;

             CREATE TABLE cp_connections (
                 id TEXT PRIMARY KEY NOT NULL,
                 workspace_id TEXT NOT NULL REFERENCES cp_workspaces(id),
                 connector_kind TEXT NOT NULL,
                 name TEXT NOT NULL,
                 config_json TEXT NOT NULL,
                 credential_ref TEXT NOT NULL CHECK (credential_ref LIKE 'cred://%'),
                 state TEXT NOT NULL CHECK (state IN ('active', 'disabled', 'retired')),
                 created_at_utc TEXT NOT NULL,
                 updated_at_utc TEXT NOT NULL
             ) STRICT;

             CREATE TABLE cp_assets (
                 id TEXT PRIMARY KEY NOT NULL,
                 workspace_id TEXT NOT NULL REFERENCES cp_workspaces(id),
                 connection_id TEXT NOT NULL REFERENCES cp_connections(id),
                 asset_kind TEXT NOT NULL,
                 name TEXT NOT NULL,
                 locator_json TEXT NOT NULL,
                 state TEXT NOT NULL CHECK (state IN ('active', 'retired')),
                 discovered_at_utc TEXT NOT NULL
             ) STRICT;

             CREATE TABLE cp_datasets (
                 id TEXT PRIMARY KEY NOT NULL,
                 workspace_id TEXT NOT NULL REFERENCES cp_workspaces(id),
                 session_id TEXT NOT NULL REFERENCES cp_sessions(id),
                 source_asset_id TEXT NOT NULL REFERENCES cp_assets(id),
                 name TEXT NOT NULL,
                 state TEXT NOT NULL CHECK (state IN ('active', 'archived')),
                 created_at_utc TEXT NOT NULL
             ) STRICT;

             CREATE TABLE cp_plans (
                 id TEXT PRIMARY KEY NOT NULL,
                 workspace_id TEXT NOT NULL REFERENCES cp_workspaces(id),
                 state TEXT NOT NULL CHECK (state IN ('active', 'archived')),
                 current_version_id TEXT,
                 created_at_utc TEXT NOT NULL,
                 updated_at_utc TEXT NOT NULL
             ) STRICT;

             CREATE TABLE cp_plan_versions (
                 id TEXT PRIMARY KEY NOT NULL,
                 plan_id TEXT NOT NULL REFERENCES cp_plans(id),
                 workspace_id TEXT NOT NULL REFERENCES cp_workspaces(id),
                 version_number INTEGER NOT NULL CHECK (version_number > 0),
                 parent_version_id TEXT REFERENCES cp_plan_versions(id),
                 logical_plan_json TEXT NOT NULL,
                 canonical_plan_bytes BLOB NOT NULL,
                 canonical_plan_digest TEXT NOT NULL CHECK (length(canonical_plan_digest) = 64),
                 plan_fingerprint TEXT NOT NULL CHECK (length(plan_fingerprint) = 64),
                 state TEXT NOT NULL CHECK (state IN ('draft', 'published', 'superseded', 'archived')),
                 created_at_utc TEXT NOT NULL,
                 published_at_utc TEXT,
                 archived_at_utc TEXT,
                 UNIQUE (plan_id, version_number)
             ) STRICT;

             CREATE TABLE cp_jobs (
                 id TEXT PRIMARY KEY NOT NULL,
                 workspace_id TEXT NOT NULL REFERENCES cp_workspaces(id),
                 session_id TEXT NOT NULL REFERENCES cp_sessions(id),
                 plan_version_id TEXT NOT NULL REFERENCES cp_plan_versions(id),
                 canonical_plan_digest TEXT NOT NULL CHECK (length(canonical_plan_digest) = 64),
                 input_json TEXT NOT NULL,
                 execution_policy_json TEXT NOT NULL,
                 output_policy_json TEXT NOT NULL,
                 state TEXT NOT NULL CHECK (state IN ('queued', 'running', 'cancelling', 'succeeded', 'failed', 'cancelled')),
                 queued_at_utc TEXT NOT NULL,
                 started_at_utc TEXT,
                 finished_at_utc TEXT,
                 run_id TEXT,
                 failure_json TEXT
             ) STRICT;

             CREATE INDEX cp_jobs_queue_index
             ON cp_jobs(workspace_id, state, queued_at_utc, id);

             CREATE INDEX cp_jobs_lifecycle_index
             ON cp_jobs(workspace_id, state, queued_at_utc DESC, id DESC);

             CREATE INDEX cp_plan_versions_concurrency_index
             ON cp_plan_versions(plan_id, state, version_number DESC, id);

             CREATE TABLE cp_runs (
                 id TEXT PRIMARY KEY NOT NULL,
                 workspace_id TEXT NOT NULL REFERENCES cp_workspaces(id),
                 session_id TEXT NOT NULL REFERENCES cp_sessions(id),
                 job_id TEXT NOT NULL UNIQUE REFERENCES cp_jobs(id),
                 plan_id TEXT NOT NULL REFERENCES cp_plans(id),
                 plan_version_id TEXT NOT NULL REFERENCES cp_plan_versions(id),
                 canonical_plan_digest TEXT NOT NULL CHECK (length(canonical_plan_digest) = 64),
                 plan_fingerprint TEXT NOT NULL CHECK (length(plan_fingerprint) = 64),
                 input_json TEXT NOT NULL,
                 engine_contract_version INTEGER NOT NULL CHECK (engine_contract_version > 0),
                 engine_build TEXT NOT NULL,
                 state TEXT NOT NULL CHECK (state IN ('running', 'cancelling', 'succeeded', 'failed', 'cancelled')),
                 started_at_utc TEXT NOT NULL,
                 finished_at_utc TEXT,
                 failure_json TEXT,
                 snapshot_ref TEXT REFERENCES snapshots(id),
                 bundle_ref TEXT REFERENCES verification_bundles(bundle_id)
             ) STRICT;

             CREATE INDEX cp_runs_lifecycle_index
             ON cp_runs(workspace_id, state, started_at_utc, id);

             CREATE TABLE cp_events (
                 event_id TEXT PRIMARY KEY NOT NULL,
                 workspace_id TEXT NOT NULL REFERENCES cp_workspaces(id),
                 session_id TEXT NOT NULL REFERENCES cp_sessions(id),
                 stream_kind TEXT NOT NULL CHECK (stream_kind IN ('job', 'run')),
                 stream_id TEXT NOT NULL,
                 sequence INTEGER NOT NULL CHECK (sequence > 0),
                 event_type TEXT NOT NULL,
                 event_version INTEGER NOT NULL CHECK (event_version > 0),
                 occurred_at_utc TEXT NOT NULL,
                 job_id TEXT NOT NULL REFERENCES cp_jobs(id),
                 run_id TEXT REFERENCES cp_runs(id),
                 request_id TEXT NOT NULL,
                 correlation_id TEXT NOT NULL,
                 actor_ref TEXT NOT NULL,
                 payload_json TEXT NOT NULL,
                 UNIQUE (stream_kind, stream_id, sequence)
             ) STRICT;

             CREATE INDEX cp_events_stream_index
             ON cp_events(workspace_id, stream_kind, stream_id, sequence);

             CREATE TABLE cp_idempotency_keys (
                 workspace_id TEXT NOT NULL REFERENCES cp_workspaces(id),
                 operation TEXT NOT NULL CHECK (operation = 'job.submit'),
                 idempotency_key TEXT NOT NULL,
                 request_digest TEXT NOT NULL CHECK (length(request_digest) = 64),
                 job_id TEXT NOT NULL UNIQUE REFERENCES cp_jobs(id),
                 result_json TEXT NOT NULL,
                 created_at_utc TEXT NOT NULL,
                 PRIMARY KEY (workspace_id, operation, idempotency_key)
             ) STRICT;

             CREATE INDEX cp_idempotency_lookup_index
             ON cp_idempotency_keys(workspace_id, operation, idempotency_key);

             CREATE TABLE cp_artifact_refs (
                 id TEXT PRIMARY KEY NOT NULL,
                 workspace_id TEXT NOT NULL REFERENCES cp_workspaces(id),
                 run_id TEXT NOT NULL REFERENCES cp_runs(id),
                 artifact_kind TEXT NOT NULL,
                 external_ref_kind TEXT NOT NULL CHECK (external_ref_kind IN ('snapshot', 'verificationBundle', 'artifact')),
                 external_ref_id TEXT NOT NULL,
                 content_digest TEXT NOT NULL CHECK (length(content_digest) = 64),
                 metadata_json TEXT NOT NULL,
                 state TEXT NOT NULL CHECK (state IN ('staged', 'committed', 'tombstoned', 'failed')),
                 created_at_utc TEXT NOT NULL,
                 committed_at_utc TEXT,
                 tombstoned_at_utc TEXT
             ) STRICT;

             CREATE INDEX cp_artifact_refs_run_index
             ON cp_artifact_refs(workspace_id, run_id, state, created_at_utc, id);

             PRAGMA user_version = 4;",
        )
        .map_err(|_| StorageError::database("apply storage migration version four"))?;
    transaction
        .commit()
        .map_err(|_| StorageError::database("commit storage migration version four"))
}

/// Version five adds the E5-J2 operation identity and terminal output set to
/// the existing Job/Run rows. The nullable operation columns preserve rows
/// created by E5-J1; those legacy rows remain readable and fail closed before
/// a new operation-specific claim because they have no typed descriptor.
fn migrate_to_version_five(connection: &mut Connection) -> Result<(), StorageError> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|_| StorageError::database("begin storage migration version five"))?;
    transaction
        .execute_batch(
            "ALTER TABLE cp_jobs ADD COLUMN operation_kind TEXT;
             ALTER TABLE cp_jobs ADD COLUMN operation_version INTEGER;
             ALTER TABLE cp_jobs ADD COLUMN operation_descriptor_json TEXT;
             ALTER TABLE cp_jobs ADD COLUMN operation_descriptor_digest TEXT;
             ALTER TABLE cp_jobs ADD COLUMN request_digest TEXT;
             ALTER TABLE cp_jobs ADD COLUMN output_refs_json TEXT NOT NULL DEFAULT '[]';

             ALTER TABLE cp_runs ADD COLUMN operation_kind TEXT;
             ALTER TABLE cp_runs ADD COLUMN operation_version INTEGER;
             ALTER TABLE cp_runs ADD COLUMN operation_descriptor_json TEXT;
             ALTER TABLE cp_runs ADD COLUMN operation_descriptor_digest TEXT;
             ALTER TABLE cp_runs ADD COLUMN output_refs_json TEXT NOT NULL DEFAULT '[]';

             CREATE TABLE cp_artifact_bodies (
                 artifact_id TEXT PRIMARY KEY NOT NULL REFERENCES cp_artifact_refs(id) ON DELETE CASCADE,
                 artifact_kind TEXT NOT NULL,
                 artifact_version INTEGER NOT NULL CHECK (artifact_version = 1),
                 content_digest TEXT NOT NULL CHECK (length(content_digest) = 64),
                 body BLOB NOT NULL,
                 provenance_json TEXT NOT NULL,
                 state TEXT NOT NULL CHECK (state IN ('staged', 'committed', 'tombstoned', 'failed')),
                 created_at_utc TEXT NOT NULL,
                 committed_at_utc TEXT
             ) STRICT;

             CREATE INDEX cp_artifact_bodies_state_index
             ON cp_artifact_bodies(state, artifact_id);

             PRAGMA user_version = 5;",
        )
        .map_err(|_| StorageError::database("apply storage migration version five"))?;
    transaction
        .commit()
        .map_err(|_| StorageError::database("commit storage migration version five"))
}

/// Version three adds the export publication journal, export manifest,
/// export tombstone, and per-file export journal tables. Existing snapshot
/// and bundle rows are untouched (ADR-004 §7 persistence plane).
fn migrate_to_version_three(connection: &mut Connection) -> Result<(), StorageError> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|_| StorageError::database("begin storage migration version three"))?;
    transaction
        .execute_batch(
            "CREATE TABLE export_publications (
                 export_id TEXT PRIMARY KEY NOT NULL,
                 snapshot_id TEXT NOT NULL,
                 destination_root TEXT NOT NULL,
                 destination_relative TEXT NOT NULL,
                 started_at_utc TEXT NOT NULL
             ) STRICT;

             CREATE TABLE export_journal (
                 export_id TEXT NOT NULL,
                 destination_root TEXT NOT NULL,
                 destination_relative TEXT NOT NULL,
                 journaled_at_utc TEXT NOT NULL
             ) STRICT;

             CREATE INDEX export_journal_export_index
             ON export_journal(export_id);

             CREATE TABLE export_manifests (
                 export_id TEXT PRIMARY KEY NOT NULL,
                 version INTEGER NOT NULL CHECK (version = 1),
                 manifest_json TEXT NOT NULL,
                 committed_at_utc TEXT NOT NULL
             ) STRICT;

             CREATE TABLE export_tombstones (
                 export_id TEXT PRIMARY KEY NOT NULL,
                 destination_root TEXT NOT NULL,
                 destination_relative TEXT NOT NULL,
                 tombstoned_at_utc TEXT NOT NULL
             ) STRICT;

             CREATE INDEX export_tombstones_cutoff_index
             ON export_tombstones(tombstoned_at_utc, export_id);

             PRAGMA user_version = 3;",
        )
        .map_err(|_| StorageError::database("apply storage migration version three"))?;
    transaction
        .commit()
        .map_err(|_| StorageError::database("commit storage migration version three"))
}

/// Version two adds verification-bundle publication journal, membership, and
/// artifact manifest tables. Existing snapshot rows are untouched.
fn migrate_to_version_two(connection: &mut Connection) -> Result<(), StorageError> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|_| StorageError::database("begin storage migration version two"))?;
    transaction
        .execute_batch(
            "CREATE TABLE bundle_publications (
                 bundle_id TEXT PRIMARY KEY NOT NULL,
                 run_id TEXT NOT NULL,
                 accepted_snapshot_id TEXT NOT NULL,
                 bundle_artifact_id TEXT NOT NULL,
                 validation_report_artifact_id TEXT NOT NULL,
                 rejected_rows_artifact_id TEXT,
                 deduplication_report_artifact_id TEXT NOT NULL,
                 started_at_utc TEXT NOT NULL
             ) STRICT;

             CREATE TABLE verification_bundles (
                 bundle_id TEXT PRIMARY KEY NOT NULL,
                 version INTEGER NOT NULL CHECK (version = 1),
                 run_id TEXT NOT NULL UNIQUE,
                 bundle_artifact_id TEXT NOT NULL UNIQUE,
                 accepted_snapshot_id TEXT NOT NULL UNIQUE,
                 validation_report_artifact_id TEXT NOT NULL UNIQUE,
                 rejected_rows_artifact_id TEXT UNIQUE,
                 deduplication_report_artifact_id TEXT NOT NULL UNIQUE,
                 membership_json TEXT NOT NULL,
                 provenance_json TEXT NOT NULL,
                 committed_at_utc TEXT NOT NULL
             ) STRICT;

             CREATE TABLE artifact_manifests (
                 artifact_id TEXT PRIMARY KEY NOT NULL,
                 bundle_id TEXT NOT NULL REFERENCES verification_bundles(bundle_id) ON DELETE CASCADE,
                 kind TEXT NOT NULL,
                 manifest_json TEXT NOT NULL,
                 provenance_json TEXT NOT NULL
             ) STRICT;

             CREATE INDEX bundle_publications_stale_index
             ON bundle_publications(started_at_utc, bundle_id);

             CREATE INDEX verification_bundles_run_index
             ON verification_bundles(run_id);

             CREATE INDEX verification_bundles_snapshot_index
             ON verification_bundles(accepted_snapshot_id);

             PRAGMA user_version = 2;",
        )
        .map_err(|_| StorageError::database("apply storage migration version two"))?;
    transaction
        .commit()
        .map_err(|_| StorageError::database("commit storage migration version two"))
}

fn migrate_to_version_one(connection: &mut Connection) -> Result<(), StorageError> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|_| StorageError::database("begin storage migration"))?;
    transaction
        .execute_batch(
            "CREATE TABLE publications (
                 snapshot_id TEXT PRIMARY KEY NOT NULL,
                 started_at_utc TEXT NOT NULL
             ) STRICT;

             CREATE TABLE snapshots (
                 id TEXT PRIMARY KEY NOT NULL,
                 version INTEGER NOT NULL CHECK (version = 1),
                 dataset_id TEXT NOT NULL,
                 session_id TEXT NOT NULL,
                 source_asset_id TEXT NOT NULL,
                 schema_json TEXT NOT NULL,
                 schema_fingerprint TEXT NOT NULL CHECK (length(schema_fingerprint) = 64),
                 row_count INTEGER NOT NULL CHECK (row_count >= 0),
                 stored_byte_count INTEGER NOT NULL CHECK (stored_byte_count >= 0),
                 partition_count INTEGER NOT NULL CHECK (partition_count >= 0),
                 lineage_json TEXT NOT NULL,
                 quality_score INTEGER CHECK (quality_score BETWEEN 0 AND 100),
                 created_at_utc TEXT NOT NULL,
                 state INTEGER NOT NULL CHECK (state IN (1, 2)),
                 tombstoned_at_utc TEXT,
                 CHECK ((state = 1 AND tombstoned_at_utc IS NULL)
                     OR (state = 2 AND tombstoned_at_utc IS NOT NULL))
             ) STRICT;

             CREATE TABLE partitions (
                 snapshot_id TEXT NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
                 sequence INTEGER NOT NULL CHECK (sequence >= 0),
                 row_count INTEGER NOT NULL CHECK (row_count > 0),
                 stored_byte_count INTEGER NOT NULL CHECK (stored_byte_count > 0),
                 sha256 TEXT NOT NULL CHECK (length(sha256) = 64),
                 PRIMARY KEY (snapshot_id, sequence)
             ) STRICT;

             CREATE INDEX snapshots_tombstone_index
             ON snapshots(state, tombstoned_at_utc, id);

             PRAGMA user_version = 1;",
        )
        .map_err(|_| StorageError::database("apply storage migration version one"))?;
    transaction
        .commit()
        .map_err(|_| StorageError::database("commit storage migration"))
}

pub(crate) fn acquire_activity(
    inner: &Arc<StoreInner>,
    kind: ActivityKind,
) -> Result<ActivityGuard, StorageError> {
    let mut state = inner
        .activity
        .lock()
        .map_err(|_| StorageError::ActivityState)?;
    if state.maintenance {
        return Err(StorageError::Busy("maintenance is active"));
    }
    match kind {
        ActivityKind::Reader => {
            if state.readers >= inner.limits.max_active_readers() {
                return Err(StorageError::Busy("active reader limit reached"));
            }
            state.readers += 1;
        }
        ActivityKind::Publisher => {
            if state.publishers >= inner.limits.max_active_publishers() {
                return Err(StorageError::Busy("active publisher limit reached"));
            }
            state.publishers += 1;
        }
        ActivityKind::ExportPublisher => {
            if state.export_publishers >= stillflow_core::MAX_ACTIVE_EXPORT_PUBLISHERS {
                return Err(StorageError::Busy("active export publisher limit reached"));
            }
            state.export_publishers += 1;
        }
    }
    drop(state);
    Ok(ActivityGuard {
        inner: Arc::clone(inner),
        kind,
        active: true,
    })
}

pub(crate) fn acquire_maintenance(
    inner: &Arc<StoreInner>,
) -> Result<MaintenanceGuard, StorageError> {
    let mut state = inner
        .activity
        .lock()
        .map_err(|_| StorageError::ActivityState)?;
    if state.maintenance
        || state.readers != 0
        || state.publishers != 0
        || state.export_publishers != 0
    {
        return Err(StorageError::Busy("storage activity prevents maintenance"));
    }
    state.maintenance = true;
    drop(state);
    Ok(MaintenanceGuard {
        inner: Arc::clone(inner),
        active: true,
    })
}

fn insert_publication(
    inner: &StoreInner,
    snapshot_id: Uuid,
    started_at: &DateTime<Utc>,
) -> Result<(), StorageError> {
    let mut connection = open_connection(inner)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|_| StorageError::database("begin publication transaction"))?;
    // Symmetric identity reservation (contract 10.5): the snapshot id maps to
    // a `partitions/<id>` directory, so it must also be free of any bundle
    // claim — pending journal rows and committed bundles alike — in addition
    // to the ordinary snapshot families.
    let existing_identity: bool = transaction
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM snapshots WHERE id = ?1)
                   OR EXISTS(SELECT 1 FROM publications WHERE snapshot_id = ?1)
                   OR EXISTS(SELECT 1 FROM bundle_publications WHERE
                              bundle_id = ?1
                           OR accepted_snapshot_id = ?1
                           OR bundle_artifact_id = ?1
                           OR validation_report_artifact_id = ?1
                           OR rejected_rows_artifact_id = ?1
                           OR deduplication_report_artifact_id = ?1)
                   OR EXISTS(SELECT 1 FROM verification_bundles WHERE
                              bundle_id = ?1
                           OR accepted_snapshot_id = ?1
                           OR bundle_artifact_id = ?1
                           OR validation_report_artifact_id = ?1
                           OR rejected_rows_artifact_id = ?1
                           OR deduplication_report_artifact_id = ?1)",
            params![snapshot_id.to_string()],
            |row| row.get(0),
        )
        .map_err(|_| StorageError::database("check existing snapshot identity"))?;
    if existing_identity {
        return Err(StorageError::AlreadyExists(snapshot_id));
    }
    transaction
        .execute(
            "INSERT INTO publications(snapshot_id, started_at_utc) VALUES (?1, ?2)",
            params![snapshot_id.to_string(), format_timestamp(started_at)],
        )
        .map_err(|_| StorageError::database("insert publication journal"))?;
    transaction
        .commit()
        .map_err(|_| StorageError::database("commit publication journal"))
}

fn write_partition(
    staging_dir: &Path,
    sequence: u32,
    envelope: &BatchEnvelope,
) -> Result<SnapshotPartition, StorageError> {
    let path = staged_partition_path(staging_dir, sequence);
    let (row_count, stored_byte_count, digest) = write_envelope_parquet(&path, envelope)?;
    SnapshotPartition::try_new(sequence, row_count, stored_byte_count, digest)
}

/// Encodes one envelope as an immutable Parquet partition file and returns
/// its row count, encoded byte length, and SHA-256 file digest.
pub(crate) fn write_envelope_parquet(
    path: &Path,
    envelope: &BatchEnvelope,
) -> Result<(u64, u64, ContentDigest), StorageError> {
    let file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(path)
        .map_err(|error| StorageError::io("create staged Parquet partition", &error))?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_row_count(Some(MAX_BATCH_ROWS))
        .build();
    let mut writer = ArrowWriter::try_new(file, envelope.payload().schema(), Some(properties))
        .map_err(|_| StorageError::parquet("initialize Parquet partition"))?;
    writer
        .write(envelope.payload())
        .map_err(|_| StorageError::parquet("encode Parquet partition"))?;
    let mut file = writer
        .into_inner()
        .map_err(|_| StorageError::parquet("finalize Parquet partition"))?;
    file.sync_all()
        .map_err(|error| StorageError::io("sync staged Parquet partition", &error))?;
    let stored_byte_count = file
        .metadata()
        .map_err(|error| StorageError::io("inspect staged Parquet partition", &error))?
        .len();
    file.seek(SeekFrom::Start(0))
        .map_err(|error| StorageError::io("rewind staged Parquet partition", &error))?;
    let digest = digest_file(&mut file)?;
    let row_count = u64::try_from(envelope.row_count())
        .map_err(|_| StorageError::ArithmeticOverflow("partition row count"))?;
    Ok((row_count, stored_byte_count, digest))
}

fn remove_staged_partition(staging_dir: &Path, sequence: u32) {
    let path = staged_partition_path(staging_dir, sequence);
    if let Ok(metadata) = fs::symlink_metadata(&path) {
        if metadata.is_file() && !metadata.file_type().is_symlink() {
            let _ = fs::remove_file(path);
        }
    }
}

fn install_partitions(
    inner: &StoreInner,
    snapshot_id: Uuid,
    manifest: &SnapshotManifest,
) -> Result<(), StorageError> {
    let final_dir = final_snapshot_dir(inner, snapshot_id);
    for partition in manifest.partitions() {
        let staged = staged_partition_path(
            &staging_snapshot_dir(inner, snapshot_id),
            partition.sequence(),
        );
        let final_path = final_partition_path(inner, snapshot_id, partition);
        fs::rename(staged, final_path)
            .map_err(|error| StorageError::io("install immutable Parquet partition", &error))?;
    }
    sync_directory(&final_dir)?;
    sync_directory(&partitions_root(inner))
}

fn create_final_snapshot_directory(
    inner: &StoreInner,
    snapshot_id: Uuid,
) -> Result<(), StorageError> {
    let final_dir = final_snapshot_dir(inner, snapshot_id);
    if fs::symlink_metadata(&final_dir).is_ok() {
        return Err(StorageError::AlreadyExists(snapshot_id));
    }
    create_exact_directory(&final_dir, "create final snapshot directory")
}

#[cfg(unix)]
pub(crate) fn sync_directory(path: &Path) -> Result<(), StorageError> {
    let directory = File::open(path)
        .map_err(|error| StorageError::io("open directory for synchronization", &error))?;
    directory
        .sync_all()
        .map_err(|error| StorageError::io("synchronize directory", &error))
}

#[cfg(not(unix))]
pub(crate) fn sync_directory(_path: &Path) -> Result<(), StorageError> {
    Ok(())
}

fn commit_manifest(inner: &StoreInner, manifest: &SnapshotManifest) -> Result<(), StorageError> {
    let mut connection = open_connection(inner)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|_| StorageError::database("begin manifest transaction"))?;
    insert_visible_snapshot(&transaction, manifest, true)?;
    transaction
        .commit()
        .map_err(|_| StorageError::database("commit visible snapshot manifest"))
}

/// Inserts one visible snapshot manifest plus its partitions and completes
/// the snapshot publication journal inside an open transaction. Bundle
/// commits reuse this with `require_publications_journal = false` because
/// their journal lives in `bundle_publications`.
pub(crate) fn insert_visible_snapshot(
    transaction: &rusqlite::Transaction<'_>,
    manifest: &SnapshotManifest,
    require_publications_journal: bool,
) -> Result<(), StorageError> {
    let snapshot = manifest.snapshot();
    let schema_json = serde_json::to_string(snapshot.schema())
        .map_err(|_| StorageError::Serialization("encode logical schema"))?;
    let lineage_json = serde_json::to_string(snapshot.lineage())
        .map_err(|_| StorageError::Serialization("encode snapshot lineage"))?;
    let stats = snapshot.stats();
    let row_count = checked_i64(stats.row_count(), "snapshot row count")?;
    let stored_byte_count = checked_i64(stats.stored_byte_count(), "snapshot stored byte count")?;
    let partition_count = i64::from(stats.partition_count());
    let quality_score = snapshot.quality_score().map(i64::from);

    // Only direct snapshot publications carry a `publications` journal row;
    // bundle children become visible through the bundle journal instead and
    // must not touch it (contract 10.4).
    if require_publications_journal {
        let journal_exists: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM publications WHERE snapshot_id = ?1)",
                params![snapshot.id().to_string()],
                |row| row.get(0),
            )
            .map_err(|_| StorageError::database("verify publication journal"))?;
        if !journal_exists {
            return Err(StorageError::InvalidManifest(
                "publication journal is missing",
            ));
        }
    }
    transaction
        .execute(
            "INSERT INTO snapshots(
                 id, version, dataset_id, session_id, source_asset_id,
                 schema_json, schema_fingerprint, row_count, stored_byte_count,
                 partition_count, lineage_json, quality_score, created_at_utc,
                 state, tombstoned_at_utc
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, NULL)",
            params![
                snapshot.id().to_string(),
                i64::from(snapshot.version()),
                snapshot.dataset_id().to_string(),
                snapshot.session_id().to_string(),
                snapshot.source_asset_id().to_string(),
                schema_json,
                snapshot.schema_fingerprint().to_string(),
                row_count,
                stored_byte_count,
                partition_count,
                lineage_json,
                quality_score,
                format_timestamp(snapshot.created_at()),
                VISIBLE_STATE,
            ],
        )
        .map_err(|_| StorageError::database("insert visible snapshot manifest"))?;
    for partition in manifest.partitions() {
        transaction
            .execute(
                "INSERT INTO partitions(
                     snapshot_id, sequence, row_count, stored_byte_count, sha256
                 ) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    snapshot.id().to_string(),
                    i64::from(partition.sequence()),
                    checked_i64(partition.row_count(), "partition row count")?,
                    checked_i64(partition.stored_byte_count(), "partition stored byte count")?,
                    partition.digest().to_string(),
                ],
            )
            .map_err(|_| StorageError::database("insert partition manifest"))?;
    }
    if require_publications_journal {
        let deleted = transaction
            .execute(
                "DELETE FROM publications WHERE snapshot_id = ?1",
                params![snapshot.id().to_string()],
            )
            .map_err(|_| StorageError::database("complete publication journal"))?;
        if deleted != 1 {
            return Err(StorageError::InvalidManifest(
                "publication journal completion count is invalid",
            ));
        }
    }
    Ok(())
}

struct RawSnapshotRow {
    version: i64,
    dataset_id: String,
    session_id: String,
    source_asset_id: String,
    schema_json: String,
    schema_fingerprint: String,
    row_count: i64,
    stored_byte_count: i64,
    partition_count: i64,
    lineage_json: String,
    quality_score: Option<i64>,
    created_at: String,
}

pub(crate) fn load_manifest_inner(
    inner: &StoreInner,
    snapshot_id: Uuid,
) -> Result<SnapshotManifest, StorageError> {
    let connection = open_connection(inner)?;
    let raw: Option<RawSnapshotRow> = connection
        .query_row(
            "SELECT version, dataset_id, session_id, source_asset_id,
                    schema_json, schema_fingerprint, row_count, stored_byte_count,
                    partition_count, lineage_json, quality_score, created_at_utc
             FROM snapshots WHERE id = ?1 AND state = ?2",
            params![snapshot_id.to_string(), VISIBLE_STATE],
            |row| {
                Ok(RawSnapshotRow {
                    version: row.get(0)?,
                    dataset_id: row.get(1)?,
                    session_id: row.get(2)?,
                    source_asset_id: row.get(3)?,
                    schema_json: row.get(4)?,
                    schema_fingerprint: row.get(5)?,
                    row_count: row.get(6)?,
                    stored_byte_count: row.get(7)?,
                    partition_count: row.get(8)?,
                    lineage_json: row.get(9)?,
                    quality_score: row.get(10)?,
                    created_at: row.get(11)?,
                })
            },
        )
        .optional()
        .map_err(|_| StorageError::database("load snapshot manifest"))?;
    let Some(raw) = raw else {
        return Err(StorageError::NotFound(snapshot_id));
    };

    let version = u16::try_from(raw.version)
        .map_err(|_| StorageError::InvalidManifest("snapshot version is invalid"))?;
    if version != DATASET_SNAPSHOT_VERSION {
        return Err(StorageError::InvalidManifest(
            "snapshot version is unsupported",
        ));
    }
    let dataset_id = parse_uuid(&raw.dataset_id, "dataset identity")?;
    let session_id = parse_uuid(&raw.session_id, "session identity")?;
    let source_asset_id = parse_uuid(&raw.source_asset_id, "source identity")?;
    let schema: LogicalSchema = serde_json::from_str(&raw.schema_json)
        .map_err(|_| StorageError::Serialization("decode logical schema"))?;
    let schema_fingerprint = LogicalSchemaFingerprint::try_from_schema(&schema)
        .map_err(|_| StorageError::InvalidManifest("logical schema fingerprint failed"))?;
    if schema_fingerprint.to_string() != raw.schema_fingerprint {
        return Err(StorageError::InvalidManifest(
            "logical schema fingerprint mismatch",
        ));
    }
    let row_count = checked_u64(raw.row_count, "snapshot row count")?;
    let stored_byte_count = checked_u64(raw.stored_byte_count, "snapshot stored byte count")?;
    if row_count > inner.limits.max_rows() {
        return Err(StorageError::InvalidManifest(
            "snapshot row count exceeds configured limit",
        ));
    }
    if stored_byte_count > inner.limits.max_stored_bytes() {
        return Err(StorageError::InvalidManifest(
            "snapshot stored byte count exceeds configured limit",
        ));
    }
    let partition_count = u32::try_from(raw.partition_count)
        .map_err(|_| StorageError::InvalidManifest("partition count is invalid"))?;
    if partition_count > inner.limits.max_partitions() {
        return Err(StorageError::InvalidManifest(
            "partition count exceeds configured limit",
        ));
    }
    let stats = SnapshotStats::try_new(row_count, stored_byte_count, partition_count)?;
    let lineage: BTreeSet<Uuid> = serde_json::from_str(&raw.lineage_json)
        .map_err(|_| StorageError::Serialization("decode snapshot lineage"))?;
    let quality_score = raw
        .quality_score
        .map(|score| {
            u8::try_from(score)
                .map_err(|_| StorageError::InvalidManifest("quality score is invalid"))
        })
        .transpose()?;
    let created_at = parse_timestamp(&raw.created_at, "snapshot creation timestamp")?;
    let snapshot = DatasetSnapshot::try_from_parts(
        version,
        snapshot_id,
        dataset_id,
        session_id,
        source_asset_id,
        schema,
        schema_fingerprint,
        stats,
        lineage,
        quality_score,
        created_at,
    )?;

    let mut statement = connection
        .prepare(
            "SELECT sequence, row_count, stored_byte_count, sha256
             FROM partitions WHERE snapshot_id = ?1 ORDER BY sequence ASC",
        )
        .map_err(|_| StorageError::database("prepare partition manifest query"))?;
    let rows = statement
        .query_map(params![snapshot_id.to_string()], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .map_err(|_| StorageError::database("query partition manifests"))?;
    let capacity = usize::try_from(partition_count)
        .map_err(|_| StorageError::InvalidManifest("partition capacity is invalid"))?;
    let mut partitions = Vec::with_capacity(capacity);
    for row in rows {
        let (sequence, rows, bytes, digest) =
            row.map_err(|_| StorageError::database("read partition manifest"))?;
        partitions.push(SnapshotPartition::try_new(
            u32::try_from(sequence)
                .map_err(|_| StorageError::InvalidManifest("partition sequence is invalid"))?,
            checked_u64(rows, "partition row count")?,
            checked_u64(bytes, "partition stored byte count")?,
            ContentDigest::try_from_hex(&digest)?,
        )?);
        if partitions.len() > capacity {
            return Err(StorageError::InvalidManifest(
                "partition rows exceed declared count",
            ));
        }
    }
    SnapshotManifest::try_new(snapshot, partitions)
}

pub(crate) fn read_partition(
    inner: &StoreInner,
    snapshot: &DatasetSnapshot,
    partition: &SnapshotPartition,
) -> Result<BatchEnvelope, StorageError> {
    let path = final_partition_path(inner, snapshot.id(), partition);
    let snapshot_directory = final_snapshot_dir(inner, snapshot.id());
    let directory_metadata = fs::symlink_metadata(&snapshot_directory)
        .map_err(|error| integrity_from_io(snapshot.id(), partition.sequence(), &error))?;
    if directory_metadata.file_type().is_symlink() {
        return Err(integrity_error(
            snapshot.id(),
            partition.sequence(),
            IntegrityFailure::Symlink,
        ));
    }
    if !directory_metadata.is_dir() {
        return Err(integrity_error(
            snapshot.id(),
            partition.sequence(),
            IntegrityFailure::NotRegularFile,
        ));
    }

    let metadata = fs::symlink_metadata(&path)
        .map_err(|error| integrity_from_io(snapshot.id(), partition.sequence(), &error))?;
    if metadata.file_type().is_symlink() {
        return Err(integrity_error(
            snapshot.id(),
            partition.sequence(),
            IntegrityFailure::Symlink,
        ));
    }
    if !metadata.is_file() {
        return Err(integrity_error(
            snapshot.id(),
            partition.sequence(),
            IntegrityFailure::NotRegularFile,
        ));
    }
    if metadata.len() != partition.stored_byte_count() {
        return Err(integrity_error(
            snapshot.id(),
            partition.sequence(),
            IntegrityFailure::LengthMismatch,
        ));
    }

    let mut file = File::open(&path)
        .map_err(|error| integrity_from_io(snapshot.id(), partition.sequence(), &error))?;
    let digest = digest_file(&mut file)?;
    if digest != partition.digest() {
        return Err(integrity_error(
            snapshot.id(),
            partition.sequence(),
            IntegrityFailure::DigestMismatch,
        ));
    }
    file.seek(SeekFrom::Start(0))
        .map_err(|error| StorageError::io("rewind verified Parquet partition", &error))?;
    let canonical_schema = logical_schema_to_arrow(snapshot.schema()).map_err(|_| {
        integrity_error(
            snapshot.id(),
            partition.sequence(),
            IntegrityFailure::SchemaMismatch,
        )
    })?;
    let options = ArrowReaderOptions::new().with_schema(Arc::clone(&canonical_schema));
    let builder =
        ParquetRecordBatchReaderBuilder::try_new_with_options(file, options).map_err(|_| {
            integrity_error(
                snapshot.id(),
                partition.sequence(),
                IntegrityFailure::InvalidParquet,
            )
        })?;
    let mut reader = builder
        .with_batch_size(MAX_BATCH_ROWS)
        .build()
        .map_err(|_| {
            integrity_error(
                snapshot.id(),
                partition.sequence(),
                IntegrityFailure::InvalidParquet,
            )
        })?;
    let batch = reader
        .next()
        .ok_or_else(|| {
            integrity_error(
                snapshot.id(),
                partition.sequence(),
                IntegrityFailure::UnexpectedBatchCount,
            )
        })?
        .map_err(|_| {
            integrity_error(
                snapshot.id(),
                partition.sequence(),
                IntegrityFailure::InvalidParquet,
            )
        })?;
    let rows = u64::try_from(batch.num_rows())
        .map_err(|_| StorageError::ArithmeticOverflow("decoded partition row count"))?;
    if rows != partition.row_count() {
        return Err(integrity_error(
            snapshot.id(),
            partition.sequence(),
            IntegrityFailure::RowCountMismatch,
        ));
    }
    if reader.next().is_some() {
        return Err(integrity_error(
            snapshot.id(),
            partition.sequence(),
            IntegrityFailure::UnexpectedBatchCount,
        ));
    }
    let batch = RecordBatch::try_new(canonical_schema, batch.columns().to_vec()).map_err(|_| {
        integrity_error(
            snapshot.id(),
            partition.sequence(),
            IntegrityFailure::SchemaMismatch,
        )
    })?;
    BatchEnvelope::try_new(
        Arc::new(snapshot.schema().clone()),
        snapshot.source_asset_id(),
        u64::from(partition.sequence()),
        batch,
    )
    .map_err(|_| {
        integrity_error(
            snapshot.id(),
            partition.sequence(),
            IntegrityFailure::SchemaMismatch,
        )
    })
}

pub(crate) fn integrity_error(
    snapshot_id: Uuid,
    sequence: u32,
    kind: IntegrityFailure,
) -> StorageError {
    StorageError::Integrity {
        snapshot_id,
        sequence,
        kind,
    }
}

fn integrity_from_io(snapshot_id: Uuid, sequence: u32, error: &std::io::Error) -> StorageError {
    if error.kind() == std::io::ErrorKind::NotFound {
        integrity_error(snapshot_id, sequence, IntegrityFailure::Missing)
    } else {
        StorageError::io("open immutable snapshot partition", error)
    }
}

fn snapshot_is_visible(inner: &StoreInner, snapshot_id: Uuid) -> Result<bool, StorageError> {
    let connection = open_connection(inner)?;
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM snapshots WHERE id = ?1 AND state = ?2)",
            params![snapshot_id.to_string(), VISIBLE_STATE],
            |row| row.get(0),
        )
        .map_err(|_| StorageError::database("check snapshot visibility"))
}

fn bundle_identity_exists(inner: &StoreInner, id: Uuid) -> Result<bool, StorageError> {
    let connection = open_connection(inner)?;
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM bundle_publications WHERE bundle_id = ?1)
              OR EXISTS(SELECT 1 FROM verification_bundles WHERE bundle_id = ?1)",
            params![id.to_string()],
            |row| row.get(0),
        )
        .map_err(|_| StorageError::database("check bundle staging ownership"))
}

fn publication_exists(inner: &StoreInner, snapshot_id: Uuid) -> Result<bool, StorageError> {
    let connection = open_connection(inner)?;
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM publications WHERE snapshot_id = ?1)",
            params![snapshot_id.to_string()],
            |row| row.get(0),
        )
        .map_err(|_| StorageError::database("check publication journal"))
}

fn stale_publications(
    inner: &StoreInner,
    cutoff: &str,
    maximum: u32,
) -> Result<Vec<Uuid>, StorageError> {
    let connection = open_connection(inner)?;
    let mut statement = connection
        .prepare(
            "SELECT snapshot_id FROM publications
             WHERE started_at_utc <= ?1 ORDER BY started_at_utc, snapshot_id LIMIT ?2",
        )
        .map_err(|_| StorageError::database("prepare stale publication query"))?;
    let rows = statement
        .query_map(params![cutoff, i64::from(maximum)], |row| {
            row.get::<_, String>(0)
        })
        .map_err(|_| StorageError::database("query stale publications"))?;
    let mut ids = Vec::with_capacity(maximum as usize);
    for row in rows {
        let value = row.map_err(|_| StorageError::database("read stale publication"))?;
        ids.push(parse_uuid(&value, "publication identity")?);
    }
    Ok(ids)
}

struct RawBundlePublicationRow {
    bundle_id: String,
    accepted_snapshot_id: String,
    bundle_artifact_id: String,
    validation_report_artifact_id: String,
    rejected_rows_artifact_id: Option<String>,
    deduplication_report_artifact_id: String,
}

struct RawBundlePublication {
    bundle_id: Uuid,
    accepted_snapshot_id: Uuid,
    bundle_artifact_id: Uuid,
    validation_report_artifact_id: Uuid,
    rejected_rows_artifact_id: Option<Uuid>,
    deduplication_report_artifact_id: Uuid,
}

fn stale_bundle_publications(
    inner: &StoreInner,
    cutoff: &str,
    maximum: u32,
) -> Result<Vec<RawBundlePublication>, StorageError> {
    let connection = open_connection(inner)?;
    let mut statement = connection
        .prepare(
            "SELECT bundle_id, accepted_snapshot_id, bundle_artifact_id,
                    validation_report_artifact_id, rejected_rows_artifact_id,
                    deduplication_report_artifact_id
             FROM bundle_publications
             WHERE started_at_utc <= ?1 ORDER BY started_at_utc, bundle_id LIMIT ?2",
        )
        .map_err(|_| StorageError::database("prepare stale bundle publication query"))?;
    let rows = statement
        .query_map(params![cutoff, i64::from(maximum)], |row| {
            Ok(RawBundlePublicationRow {
                bundle_id: row.get::<_, String>(0)?,
                accepted_snapshot_id: row.get::<_, String>(1)?,
                bundle_artifact_id: row.get::<_, String>(2)?,
                validation_report_artifact_id: row.get::<_, String>(3)?,
                rejected_rows_artifact_id: row.get::<_, Option<String>>(4)?,
                deduplication_report_artifact_id: row.get::<_, String>(5)?,
            })
        })
        .map_err(|_| StorageError::database("query stale bundle publications"))?;
    let mut publications = Vec::new();
    for row in rows {
        let raw = row.map_err(|_| StorageError::database("read stale bundle publication"))?;
        let parse = |value: String, label: &'static str| -> Result<Uuid, StorageError> {
            Uuid::parse_str(&value).map_err(|_| StorageError::InvalidManifest(label))
        };
        publications.push(RawBundlePublication {
            bundle_id: parse(raw.bundle_id, "bundle identity")?,
            accepted_snapshot_id: parse(raw.accepted_snapshot_id, "accepted snapshot identity")?,
            bundle_artifact_id: parse(raw.bundle_artifact_id, "bundle artifact identity")?,
            validation_report_artifact_id: parse(
                raw.validation_report_artifact_id,
                "validation report artifact identity",
            )?,
            rejected_rows_artifact_id: raw
                .rejected_rows_artifact_id
                .map(|value| parse(value, "rejected rows artifact identity"))
                .transpose()?,
            deduplication_report_artifact_id: parse(
                raw.deduplication_report_artifact_id,
                "deduplication report artifact identity",
            )?,
        });
    }
    Ok(publications)
}

/// Recovers stale verification-bundle publications under the maintenance
/// gate (contract 10.4). A committed journal row without a visible bundle is
/// rolled back together with its staging and any installed artifact
/// directories; a visible bundle only loses staging residue.
fn recover_bundles(
    inner: &StoreInner,
    cutoff: &str,
    maximum: u32,
    report: &mut RecoveryReport,
) -> Result<(), StorageError> {
    for publication in stale_bundle_publications(inner, cutoff, maximum)? {
        checked_increment(&mut report.examined, "recovery examined count")?;
        let bundle_visible = bundle_is_visible(inner, publication.bundle_id)?;
        if !bundle_visible {
            remove_uuid_directory(
                &staging_root(inner),
                publication.bundle_id,
                SymlinkPolicy::Ignore,
                "remove unpublished bundle staging",
            )?;
            for artifact_id in [
                Some(publication.accepted_snapshot_id),
                Some(publication.bundle_artifact_id),
                Some(publication.validation_report_artifact_id),
                publication.rejected_rows_artifact_id,
                Some(publication.deduplication_report_artifact_id),
            ]
            .into_iter()
            .flatten()
            {
                // Per-identity ownership guard: another claimant may have
                // committed this id after the stale journal row was written
                // (ordinary snapshot or a different bundle). Recovery never
                // deletes a directory whose id is now visible or owned by any
                // other live publication (contract 10.4; V30 safety).
                if identity_owned_elsewhere(inner, artifact_id, publication.bundle_id)? {
                    checked_increment(&mut report.ignored, "recovery ignored count")?;
                    continue;
                }
                remove_uuid_directory(
                    &partitions_root(inner),
                    artifact_id,
                    SymlinkPolicy::Ignore,
                    "remove unpublished bundle artifact directory",
                )?;
            }
        } else {
            remove_uuid_directory(
                &staging_root(inner),
                publication.bundle_id,
                SymlinkPolicy::Ignore,
                "remove committed bundle staging residue",
            )?;
        }
        delete_bundle_publication(inner, publication.bundle_id)?;
        checked_increment(&mut report.recovered, "recovery recovered count")?;
    }
    Ok(())
}

/// Returns `true` when `id` is owned by anything other than the stale bundle
/// being recovered: an ordinary snapshot row, or another bundle's committed
/// row or pending journal claim over the same identity.
fn identity_owned_elsewhere(
    inner: &StoreInner,
    id: Uuid,
    exclude_bundle_id: Uuid,
) -> Result<bool, StorageError> {
    let connection = open_connection(inner)?;
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM snapshots WHERE id = ?1)
                   OR EXISTS(SELECT 1 FROM verification_bundles WHERE
                              bundle_id <> ?2
                           AND (bundle_id = ?1
                             OR accepted_snapshot_id = ?1
                             OR bundle_artifact_id = ?1
                             OR validation_report_artifact_id = ?1
                             OR rejected_rows_artifact_id = ?1
                             OR deduplication_report_artifact_id = ?1))
                   OR EXISTS(SELECT 1 FROM bundle_publications WHERE
                              bundle_id <> ?2
                           AND (bundle_id = ?1
                             OR accepted_snapshot_id = ?1
                             OR bundle_artifact_id = ?1
                             OR validation_report_artifact_id = ?1
                             OR rejected_rows_artifact_id = ?1
                             OR deduplication_report_artifact_id = ?1))",
            params![id.to_string(), exclude_bundle_id.to_string()],
            |row| row.get(0),
        )
        .map_err(|_| StorageError::database("check bundle identity ownership"))
}

fn bundle_is_visible(inner: &StoreInner, bundle_id: Uuid) -> Result<bool, StorageError> {
    let connection = open_connection(inner)?;
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM verification_bundles WHERE bundle_id = ?1)",
            params![bundle_id.to_string()],
            |row| row.get(0),
        )
        .map_err(|_| StorageError::database("check bundle visibility"))
}

fn delete_bundle_publication(inner: &StoreInner, bundle_id: Uuid) -> Result<(), StorageError> {
    let connection = open_connection(inner)?;
    connection
        .execute(
            "DELETE FROM bundle_publications WHERE bundle_id = ?1",
            params![bundle_id.to_string()],
        )
        .map(|_| ())
        .map_err(|_| StorageError::database("delete bundle publication journal"))
}

fn eligible_tombstones(
    inner: &StoreInner,
    cutoff: &str,
    maximum: u32,
) -> Result<Vec<Uuid>, StorageError> {
    let connection = open_connection(inner)?;
    let mut statement = connection
        .prepare(
            "SELECT id FROM snapshots
             WHERE state = ?1 AND tombstoned_at_utc <= ?2
             ORDER BY tombstoned_at_utc, id LIMIT ?3",
        )
        .map_err(|_| StorageError::database("prepare tombstone query"))?;
    let rows = statement
        .query_map(
            params![TOMBSTONED_STATE, cutoff, i64::from(maximum)],
            |row| row.get::<_, String>(0),
        )
        .map_err(|_| StorageError::database("query eligible tombstones"))?;
    let mut ids = Vec::with_capacity(maximum as usize);
    for row in rows {
        let value = row.map_err(|_| StorageError::database("read eligible tombstone"))?;
        ids.push(parse_uuid(&value, "tombstoned snapshot identity")?);
    }
    Ok(ids)
}

fn scan_orphan_staging(
    inner: &StoreInner,
    maximum: u32,
    report: &mut RecoveryReport,
) -> Result<(), StorageError> {
    let entries = fs::read_dir(staging_root(inner))
        .map_err(|error| StorageError::io("scan staging root", &error))?;
    let mut scanned = 0_u32;
    for entry in entries {
        if scanned >= maximum {
            break;
        }
        checked_increment(&mut scanned, "staging scan count")?;
        let entry = entry.map_err(|error| StorageError::io("read staging entry", &error))?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            checked_increment(&mut report.ignored, "recovery ignored count")?;
            continue;
        };
        let Ok(snapshot_id) = Uuid::parse_str(&name) else {
            checked_increment(&mut report.ignored, "recovery ignored count")?;
            continue;
        };
        checked_increment(&mut report.examined, "recovery examined count")?;
        if bundle_identity_exists(inner, snapshot_id)? {
            // Bundle-owned staging is handled by `recover_bundles` when its
            // publication is stale; it is never an orphan here.
            continue;
        }
        if publication_exists(inner, snapshot_id)? {
            checked_increment(&mut report.ignored, "recovery ignored count")?;
            continue;
        }
        let outcome = remove_uuid_directory(
            &staging_root(inner),
            snapshot_id,
            SymlinkPolicy::Ignore,
            "remove orphan staging directory",
        )?;
        match outcome {
            RemovalOutcome::Removed | RemovalOutcome::Missing => {
                checked_increment(&mut report.recovered, "recovery recovered count")?;
            }
            RemovalOutcome::Ignored => {
                checked_increment(&mut report.ignored, "recovery ignored count")?;
            }
        }
    }
    Ok(())
}

fn delete_publication(inner: &StoreInner, snapshot_id: Uuid) -> Result<(), StorageError> {
    let connection = open_connection(inner)?;
    connection
        .execute(
            "DELETE FROM publications WHERE snapshot_id = ?1",
            params![snapshot_id.to_string()],
        )
        .map(|_| ())
        .map_err(|_| StorageError::database("delete publication journal"))
}

fn abort_publication(inner: &StoreInner, snapshot_id: Uuid) {
    let Ok(connection) = open_connection(inner) else {
        return;
    };
    let _ = connection.execute(
        "DELETE FROM publications WHERE snapshot_id = ?1",
        params![snapshot_id.to_string()],
    );
}

/// Best-effort removal of one bundle publication journal row.
pub(crate) fn abort_bundle_publication(inner: &StoreInner, bundle_id: Uuid) {
    let Ok(connection) = open_connection(inner) else {
        return;
    };
    let _ = connection.execute(
        "DELETE FROM bundle_publications WHERE bundle_id = ?1",
        params![bundle_id.to_string()],
    );
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RemovalOutcome {
    Removed,
    Missing,
    Ignored,
}

#[derive(Clone, Copy)]
enum SymlinkPolicy {
    Reject,
    Ignore,
}

fn remove_uuid_directory(
    parent: &Path,
    snapshot_id: Uuid,
    symlink_policy: SymlinkPolicy,
    operation: &'static str,
) -> Result<RemovalOutcome, StorageError> {
    let path = parent.join(snapshot_id.to_string());
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(RemovalOutcome::Missing)
        }
        Err(error) => return Err(StorageError::io(operation, &error)),
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return match symlink_policy {
            SymlinkPolicy::Ignore => Ok(RemovalOutcome::Ignored),
            SymlinkPolicy::Reject => Err(StorageError::InvalidManifest(
                "managed snapshot directory is not a regular directory",
            )),
        };
    }
    fs::remove_dir_all(path).map_err(|error| StorageError::io(operation, &error))?;
    Ok(RemovalOutcome::Removed)
}

fn validate_maintenance_bound(maximum: u32) -> Result<(), StorageError> {
    if maximum == 0 || maximum > MAX_MAINTENANCE_CANDIDATES {
        return Err(StorageError::InvalidConfiguration(
            "maintenance candidate limit is outside the supported range",
        ));
    }
    Ok(())
}

fn cutoff_timestamp(
    now: DateTime<Utc>,
    duration: Duration,
    operation: &'static str,
) -> Result<String, StorageError> {
    let duration = chrono::Duration::from_std(duration)
        .map_err(|_| StorageError::InvalidTimestampOrder(operation))?;
    let cutoff = now
        .checked_sub_signed(duration)
        .ok_or(StorageError::InvalidTimestampOrder(operation))?;
    Ok(format_timestamp(&cutoff))
}

pub(crate) fn format_timestamp(timestamp: &DateTime<Utc>) -> String {
    timestamp.to_rfc3339_opts(SecondsFormat::Nanos, true)
}

pub(crate) fn parse_timestamp(
    value: &str,
    operation: &'static str,
) -> Result<DateTime<Utc>, StorageError> {
    DateTime::parse_from_rfc3339(value)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .map_err(|_| StorageError::InvalidManifest(operation))
}

fn parse_uuid(value: &str, operation: &'static str) -> Result<Uuid, StorageError> {
    Uuid::parse_str(value).map_err(|_| StorageError::InvalidManifest(operation))
}

fn checked_i64(value: u64, operation: &'static str) -> Result<i64, StorageError> {
    i64::try_from(value).map_err(|_| StorageError::ArithmeticOverflow(operation))
}

fn checked_u64(value: i64, operation: &'static str) -> Result<u64, StorageError> {
    u64::try_from(value).map_err(|_| StorageError::InvalidManifest(operation))
}

fn checked_increment(value: &mut u32, operation: &'static str) -> Result<(), StorageError> {
    *value = value
        .checked_add(1)
        .ok_or(StorageError::ArithmeticOverflow(operation))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::io::Write;

    use arrow_array::{Array, Int64Array, RecordBatch};
    use tempfile::TempDir;

    use stillflow_core::{logical_schema_to_arrow, ColumnId, LogicalField, LogicalType};

    use super::*;

    fn at(second: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(second, 0).expect("valid timestamp")
    }

    fn logical_schema(column_id: u128, name: &str) -> Arc<LogicalSchema> {
        Arc::new(
            LogicalSchema::new(vec![LogicalField::new(
                ColumnId::from_uuid(Uuid::from_u128(column_id)),
                name,
                LogicalType::Int64,
                false,
            )
            .expect("valid field")])
            .expect("valid schema"),
        )
    }

    fn draft(snapshot_id: Uuid, source_asset_id: Uuid, schema: &LogicalSchema) -> SnapshotDraft {
        SnapshotDraft::try_new(
            snapshot_id,
            Uuid::from_u128(2),
            Uuid::from_u128(3),
            source_asset_id,
            schema.clone(),
            BTreeSet::from([Uuid::from_u128(9)]),
            Some(97),
            at(1_700_000_000),
        )
        .expect("valid draft")
    }

    fn envelope(
        schema: Arc<LogicalSchema>,
        source_asset_id: Uuid,
        sequence: u64,
        values: Vec<i64>,
    ) -> BatchEnvelope {
        let arrow_schema = logical_schema_to_arrow(&schema).expect("Arrow schema");
        let batch = RecordBatch::try_new(arrow_schema, vec![Arc::new(Int64Array::from(values))])
            .expect("record batch");
        BatchEnvelope::try_new(schema, source_asset_id, sequence, batch).expect("envelope")
    }

    fn store(temp: &TempDir) -> SnapshotStore {
        SnapshotStore::open(temp.path(), StorageLimits::default()).expect("open store")
    }

    fn publish(
        store: &SnapshotStore,
        snapshot_id: Uuid,
        source_asset_id: Uuid,
        schema: Arc<LogicalSchema>,
        partitions: Vec<Vec<i64>>,
    ) -> SnapshotManifest {
        let mut writer = store
            .begin_snapshot(
                draft(snapshot_id, source_asset_id, &schema),
                at(1_700_000_001),
            )
            .expect("begin snapshot");
        for (sequence, values) in partitions.into_iter().enumerate() {
            writer
                .append(&envelope(
                    Arc::clone(&schema),
                    source_asset_id,
                    u64::try_from(sequence).expect("test sequence"),
                    values,
                ))
                .expect("append envelope");
        }
        writer.commit().expect("commit snapshot")
    }

    fn collect_values(reader: SnapshotBatchReader) -> Vec<i64> {
        let mut values = Vec::new();
        for envelope in reader {
            let envelope = envelope.expect("valid stored envelope");
            let array = envelope
                .payload()
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 column");
            values.extend(array.values().iter().copied());
        }
        values
    }

    fn open_store_for_test(temp: &TempDir) -> SnapshotStore {
        SnapshotStore::open(temp.path(), StorageLimits::default()).expect("store")
    }

    #[test]
    fn migration_is_idempotent_and_future_versions_fail_closed() {
        let temp = TempDir::new().expect("temp directory");
        let first = store(&temp);
        drop(first);
        let second = store(&temp);
        drop(second);
        let connection =
            Connection::open(temp.path().join("metadata.sqlite3")).expect("open metadata database");
        let version: i64 = connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .expect("read version");
        assert_eq!(version, 7);

        // A legacy version-one database migrates through the current schema
        // and gains the bundle, export, and control-plane tables.
        let legacy = TempDir::new().expect("legacy temp directory");
        let connection =
            Connection::open(legacy.path().join("metadata.sqlite3")).expect("legacy database");
        connection
            .execute_batch(
                "CREATE TABLE publications (
                     snapshot_id TEXT PRIMARY KEY NOT NULL,
                     started_at_utc TEXT NOT NULL
                 ) STRICT;
                 CREATE TABLE snapshots (
                     id TEXT PRIMARY KEY NOT NULL,
                     version INTEGER NOT NULL CHECK (version = 1),
                     dataset_id TEXT NOT NULL,
                     session_id TEXT NOT NULL,
                     source_asset_id TEXT NOT NULL,
                     schema_json TEXT NOT NULL,
                     schema_fingerprint TEXT NOT NULL CHECK (length(schema_fingerprint) = 64),
                     row_count INTEGER NOT NULL CHECK (row_count >= 0),
                     stored_byte_count INTEGER NOT NULL CHECK (stored_byte_count >= 0),
                     partition_count INTEGER NOT NULL CHECK (partition_count >= 0),
                     lineage_json TEXT NOT NULL,
                     quality_score INTEGER CHECK (quality_score BETWEEN 0 AND 100),
                     created_at_utc TEXT NOT NULL,
                     state INTEGER NOT NULL CHECK (state IN (1, 2)),
                     tombstoned_at_utc TEXT,
                     CHECK ((state = 1 AND tombstoned_at_utc IS NULL)
                         OR (state = 2 AND tombstoned_at_utc IS NOT NULL))
                 ) STRICT;
                 CREATE TABLE partitions (
                     snapshot_id TEXT NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
                     sequence INTEGER NOT NULL CHECK (sequence >= 0),
                     row_count INTEGER NOT NULL CHECK (row_count > 0),
                     stored_byte_count INTEGER NOT NULL CHECK (stored_byte_count > 0),
                     sha256 TEXT NOT NULL CHECK (length(sha256) = 64),
                     PRIMARY KEY (snapshot_id, sequence)
                 ) STRICT;
                 CREATE INDEX snapshots_tombstone_index
                 ON snapshots(state, tombstoned_at_utc, id);
                 PRAGMA user_version = 1;",
            )
            .expect("create legacy v1 database");
        drop(connection);
        drop(open_store_for_test(&legacy));
        let connection =
            Connection::open(legacy.path().join("metadata.sqlite3")).expect("migrated database");
        let version: i64 = connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .expect("read migrated version");
        assert_eq!(version, 7);
        for table in [
            "bundle_publications",
            "verification_bundles",
            "artifact_manifests",
            "export_publications",
            "export_journal",
            "export_manifests",
            "export_tombstones",
            "cp_artifact_bodies",
            "qd1_profile_history",
            "qd1_drift_comparisons",
        ] {
            let present: i64 = connection
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
                    [table],
                    |row| row.get(0),
                )
                .expect("table lookup");
            assert_eq!(present, 1, "table {table} must exist after migration");
        }
        drop(connection);

        let future = TempDir::new().expect("future temp directory");
        let connection = Connection::open(future.path().join("metadata.sqlite3"))
            .expect("create future database");
        connection
            .execute_batch("PRAGMA user_version = 8;")
            .expect("set future version");
        drop(connection);
        assert!(matches!(
            SnapshotStore::open(future.path(), StorageLimits::default()),
            Err(StorageError::UnsupportedStorageVersion(8))
        ));
        let connection = Connection::open(future.path().join("metadata.sqlite3"))
            .expect("reopen future database");
        let version: i64 = connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .expect("read unchanged version");
        assert_eq!(version, 8);
    }

    #[test]
    fn version_four_migration_preserves_snapshot_bundle_artifact_and_export_rows() {
        let temp = TempDir::new().expect("temp directory");
        let mut connection = Connection::open(temp.path().join("metadata.sqlite3"))
            .expect("create version-three database");
        migrate_to_version_one(&mut connection).expect("v1 schema");
        migrate_to_version_two(&mut connection).expect("v2 schema");
        migrate_to_version_three(&mut connection).expect("v3 schema");
        let snapshot_id = Uuid::from_u128(1).to_string();
        let bundle_id = Uuid::from_u128(2).to_string();
        let run_id = Uuid::from_u128(3).to_string();
        let artifact_id = Uuid::from_u128(4).to_string();
        let timestamp = "2025-01-01T00:00:00.000000000Z";
        connection
            .execute(
                "INSERT INTO snapshots
                 (id, version, dataset_id, session_id, source_asset_id, schema_json,
                  schema_fingerprint, row_count, stored_byte_count, partition_count,
                  lineage_json, quality_score, created_at_utc, state, tombstoned_at_utc)
                 VALUES (?1, 1, ?2, ?3, ?4, '{}', ?5, 0, 0, 0, '[]', NULL, ?6, 1, NULL)",
                params![
                    snapshot_id,
                    Uuid::from_u128(5).to_string(),
                    Uuid::from_u128(6).to_string(),
                    Uuid::from_u128(7).to_string(),
                    "0".repeat(64),
                    timestamp
                ],
            )
            .expect("legacy Snapshot row");
        connection
            .execute(
                "INSERT INTO verification_bundles
                 (bundle_id, version, run_id, bundle_artifact_id, accepted_snapshot_id,
                  validation_report_artifact_id, rejected_rows_artifact_id,
                  deduplication_report_artifact_id, membership_json, provenance_json,
                  committed_at_utc)
                 VALUES (?1, 1, ?2, ?3, ?4, ?5, NULL, ?6, '{}', '{}', ?7)",
                params![
                    bundle_id,
                    run_id,
                    artifact_id,
                    snapshot_id,
                    Uuid::from_u128(8).to_string(),
                    Uuid::from_u128(9).to_string(),
                    timestamp
                ],
            )
            .expect("legacy Bundle row");
        connection
            .execute(
                "INSERT INTO artifact_manifests
                 (artifact_id, bundle_id, kind, manifest_json, provenance_json)
                 VALUES (?1, ?2, 'acceptedSnapshot', '{}', '{}')",
                params![artifact_id, bundle_id],
            )
            .expect("legacy Artifact row");
        connection
            .execute(
                "INSERT INTO export_manifests
                 (export_id, version, manifest_json, committed_at_utc)
                 VALUES (?1, 1, '{}', ?2)",
                params![Uuid::from_u128(10).to_string(), timestamp],
            )
            .expect("legacy Export row");
        drop(connection);

        drop(SnapshotStore::open(temp.path(), StorageLimits::default()).expect("v4 migration"));
        let connection = Connection::open(temp.path().join("metadata.sqlite3"))
            .expect("reopen migrated database");
        let version: i64 = connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .expect("read migrated version");
        assert_eq!(version, 7);
        for (table, expected) in [
            ("snapshots", 1_i64),
            ("verification_bundles", 1_i64),
            ("artifact_manifests", 1_i64),
            ("export_manifests", 1_i64),
        ] {
            let count: i64 = connection
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .expect("count preserved rows");
            assert_eq!(count, expected, "preserved rows in {table}");
        }
    }

    #[test]
    fn managed_root_has_one_independent_owner() {
        let temp = TempDir::new().expect("temp directory");
        let first = store(&temp);
        assert!(matches!(
            SnapshotStore::open(temp.path(), StorageLimits::default()),
            Err(StorageError::Busy(_))
        ));
        let shared = first.clone();
        drop(first);
        assert!(matches!(
            SnapshotStore::open(temp.path(), StorageLimits::default()),
            Err(StorageError::Busy(_))
        ));
        drop(shared);
        store(&temp);
    }

    #[test]
    fn manifest_loading_reapplies_configured_and_batch_bounds() {
        let temp = TempDir::new().expect("temp directory");
        let store = store(&temp);
        let snapshot_id = Uuid::from_u128(1);
        let source = Uuid::from_u128(4);
        let schema = logical_schema(11, "value");
        publish(&store, snapshot_id, source, schema, vec![vec![1, 2]]);
        drop(store);

        let row_limits = StorageLimits::try_new(8, 8, 1, crate::MAX_SNAPSHOT_STORED_BYTES, 1, 1)
            .expect("row limits");
        let row_limited = SnapshotStore::open(temp.path(), row_limits).expect("row-limited store");
        assert!(matches!(
            row_limited.load_manifest(snapshot_id),
            Err(StorageError::InvalidManifest(
                "snapshot row count exceeds configured limit"
            ))
        ));
        drop(row_limited);

        let byte_limits =
            StorageLimits::try_new(8, 8, crate::MAX_SNAPSHOT_ROWS, 1, 1, 1).expect("byte limits");
        let byte_limited =
            SnapshotStore::open(temp.path(), byte_limits).expect("byte-limited store");
        assert!(matches!(
            byte_limited.load_manifest(snapshot_id),
            Err(StorageError::InvalidManifest(
                "snapshot stored byte count exceeds configured limit"
            ))
        ));

        let digest = ContentDigest::try_from_hex(&"00".repeat(32)).expect("digest");
        assert!(matches!(
            SnapshotPartition::try_new(
                0,
                u64::try_from(MAX_BATCH_ROWS).expect("batch rows") + 1,
                1,
                digest,
            ),
            Err(StorageError::InvalidManifest(
                "partition row count exceeds the batch limit"
            ))
        ));
    }

    #[test]
    fn snapshot_is_invisible_until_commit_and_roundtrips_exactly() {
        let temp = TempDir::new().expect("temp directory");
        let store = store(&temp);
        let snapshot_id = Uuid::from_u128(1);
        let source = Uuid::from_u128(4);
        let schema = logical_schema(11, "value");
        let mut writer = store
            .begin_snapshot(draft(snapshot_id, source, &schema), at(1_700_000_001))
            .expect("begin snapshot");
        writer
            .append(&envelope(Arc::clone(&schema), source, 0, vec![1, 2]))
            .expect("append first");
        assert!(matches!(
            store.load_manifest(snapshot_id),
            Err(StorageError::NotFound(id)) if id == snapshot_id
        ));
        writer
            .append(&envelope(Arc::clone(&schema), source, 1, vec![3]))
            .expect("append second");
        let manifest = writer.commit().expect("commit");

        assert_eq!(manifest.snapshot().schema(), schema.as_ref());
        assert_eq!(manifest.snapshot().source_asset_id(), source);
        assert_eq!(
            manifest.snapshot().lineage(),
            &BTreeSet::from([Uuid::from_u128(9)])
        );
        assert_eq!(manifest.snapshot().quality_score(), Some(97));
        assert_eq!(manifest.snapshot().stats().row_count(), 3);
        assert_eq!(manifest.snapshot().stats().partition_count(), 2);
        assert_eq!(
            manifest.snapshot().stats().stored_byte_count(),
            manifest
                .partitions()
                .iter()
                .map(SnapshotPartition::stored_byte_count)
                .sum::<u64>()
        );
        assert_eq!(
            collect_values(store.read_batches(snapshot_id).expect("reader")),
            vec![1, 2, 3]
        );
        assert_eq!(
            store.verify_snapshot(snapshot_id).expect("verify"),
            manifest
        );
        assert!(matches!(
            store.begin_snapshot(
                draft(snapshot_id, source, &schema),
                at(1_700_000_002)
            ),
            Err(StorageError::AlreadyExists(id)) if id == snapshot_id
        ));
    }

    #[test]
    fn empty_envelopes_create_no_physical_partitions() {
        let temp = TempDir::new().expect("temp directory");
        let store = store(&temp);
        let source = Uuid::from_u128(4);
        let schema = logical_schema(11, "value");
        let manifest = publish(
            &store,
            Uuid::from_u128(1),
            source,
            Arc::clone(&schema),
            vec![Vec::new(), Vec::new()],
        );
        assert!(manifest.partitions().is_empty());
        assert_eq!(manifest.snapshot().stats().row_count(), 0);
        assert_eq!(manifest.snapshot().stats().stored_byte_count(), 0);
        assert!(store
            .read_batches(manifest.snapshot().id())
            .expect("empty reader")
            .next()
            .is_none());
        let final_dir = final_snapshot_dir(&store.inner, manifest.snapshot().id());
        assert_eq!(
            fs::read_dir(final_dir)
                .expect("read final directory")
                .count(),
            0
        );
    }

    #[test]
    fn alternate_batch_partitions_preserve_ordered_rows() {
        let temp = TempDir::new().expect("temp directory");
        let store = store(&temp);
        let source = Uuid::from_u128(4);
        let schema = logical_schema(11, "value");
        publish(
            &store,
            Uuid::from_u128(1),
            source,
            Arc::clone(&schema),
            vec![vec![1], vec![2, 3], vec![4]],
        );
        publish(
            &store,
            Uuid::from_u128(2),
            source,
            schema,
            vec![vec![1, 2, 3, 4]],
        );
        let first = collect_values(store.read_batches(Uuid::from_u128(1)).expect("first"));
        let second = collect_values(store.read_batches(Uuid::from_u128(2)).expect("second"));
        assert_eq!(first, second);
    }

    #[test]
    fn rejects_sequence_lineage_schema_and_configured_bounds() {
        let temp = TempDir::new().expect("temp directory");
        let store = store(&temp);
        let source = Uuid::from_u128(4);
        let schema = logical_schema(11, "value");

        let mut sequence = store
            .begin_snapshot(
                draft(Uuid::from_u128(1), source, &schema),
                at(1_700_000_001),
            )
            .expect("sequence writer");
        assert!(matches!(
            sequence.append(&envelope(Arc::clone(&schema), source, 1, vec![1])),
            Err(StorageError::Sequence {
                expected: 0,
                actual: 1
            })
        ));
        drop(sequence);

        let mut lineage = store
            .begin_snapshot(
                draft(Uuid::from_u128(2), source, &schema),
                at(1_700_000_001),
            )
            .expect("lineage writer");
        assert!(matches!(
            lineage.append(&envelope(
                Arc::clone(&schema),
                Uuid::from_u128(8),
                0,
                vec![1]
            )),
            Err(StorageError::LineageMismatch { sequence: 0 })
        ));
        drop(lineage);

        let other_schema = logical_schema(12, "other");
        let mut drift = store
            .begin_snapshot(
                draft(Uuid::from_u128(3), source, &schema),
                at(1_700_000_001),
            )
            .expect("drift writer");
        assert!(matches!(
            drift.append(&envelope(other_schema, source, 0, vec![1])),
            Err(StorageError::SchemaDrift { sequence: 0 })
        ));
        drop(drift);

        let limits = StorageLimits::try_new(1, 1, 2, 1, 1, 1).expect("small limits");
        let limited_temp = TempDir::new().expect("limited temp directory");
        let limited = SnapshotStore::open(limited_temp.path(), limits).expect("limited store");
        let mut rows = limited
            .begin_snapshot(
                draft(Uuid::from_u128(4), source, &schema),
                at(1_700_000_001),
            )
            .expect("row writer");
        assert!(matches!(
            rows.append(&envelope(Arc::clone(&schema), source, 0, vec![1, 2, 3])),
            Err(StorageError::RowLimitExceeded { .. })
        ));
        drop(rows);

        let mut bytes = limited
            .begin_snapshot(
                draft(Uuid::from_u128(5), source, &schema),
                at(1_700_000_001),
            )
            .expect("byte writer");
        assert!(matches!(
            bytes.append(&envelope(Arc::clone(&schema), source, 0, vec![1])),
            Err(StorageError::StoredByteLimitExceeded { .. })
        ));
        drop(bytes);

        let mut envelopes = limited
            .begin_snapshot(
                draft(Uuid::from_u128(6), source, &schema),
                at(1_700_000_001),
            )
            .expect("envelope writer");
        envelopes
            .append(&envelope(Arc::clone(&schema), source, 0, Vec::new()))
            .expect("first empty envelope");
        assert!(matches!(
            envelopes.append(&envelope(schema, source, 1, Vec::new())),
            Err(StorageError::EnvelopeLimitExceeded { .. })
        ));
    }

    #[test]
    fn checksum_missing_file_and_lazy_reader_fail_closed_without_paths() {
        let temp = TempDir::new().expect("temp directory");
        let store = store(&temp);
        let source = Uuid::from_u128(4);
        let schema = logical_schema(11, "value");
        let first_id = Uuid::from_u128(1);
        let manifest = publish(
            &store,
            first_id,
            source,
            Arc::clone(&schema),
            vec![vec![1], vec![2]],
        );
        let second_path = final_partition_path(&store.inner, first_id, &manifest.partitions()[1]);
        let mut file = OpenOptions::new()
            .write(true)
            .open(&second_path)
            .expect("open partition for corruption");
        file.seek(SeekFrom::Start(0)).expect("rewind partition");
        file.write_all(&[0]).expect("corrupt partition");
        file.sync_all().expect("sync corruption");
        drop(file);

        let mut reader = store.read_batches(first_id).expect("lazy reader");
        assert!(reader.next().expect("first partition").is_ok());
        drop(reader);
        let mut reader = store.read_batches(first_id).expect("second reader");
        assert!(reader.next().expect("first partition").is_ok());
        let error = reader
            .next()
            .expect("second partition")
            .expect_err("digest mismatch");
        assert!(matches!(
            &error,
            StorageError::Integrity {
                kind: IntegrityFailure::DigestMismatch,
                ..
            }
        ));
        let display = error.to_string();
        let debug = format!("{error:?}");
        assert!(!display.contains(&temp.path().display().to_string()));
        assert!(!debug.contains(&temp.path().display().to_string()));

        let missing_id = Uuid::from_u128(2);
        let missing = publish(&store, missing_id, source, schema, vec![vec![7]]);
        fs::remove_file(final_partition_path(
            &store.inner,
            missing_id,
            &missing.partitions()[0],
        ))
        .expect("remove partition");
        assert!(matches!(
            store
                .read_batches(missing_id)
                .expect("missing reader")
                .next(),
            Some(Err(StorageError::Integrity {
                kind: IntegrityFailure::Missing,
                ..
            }))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_partition_fails_closed() {
        use std::os::unix::fs::symlink;

        let temp = TempDir::new().expect("temp directory");
        let store = store(&temp);
        let source = Uuid::from_u128(4);
        let schema = logical_schema(11, "value");
        let snapshot_id = Uuid::from_u128(1);
        let manifest = publish(&store, snapshot_id, source, schema, vec![vec![1]]);
        let path = final_partition_path(&store.inner, snapshot_id, &manifest.partitions()[0]);
        fs::remove_file(&path).expect("remove original file");
        symlink("/dev/null", path).expect("create symlink");
        assert!(matches!(
            store.read_batches(snapshot_id).expect("reader").next(),
            Some(Err(StorageError::Integrity {
                kind: IntegrityFailure::Symlink,
                ..
            }))
        ));
    }

    #[test]
    fn recovery_removes_precommit_files_and_preserves_committed_snapshot() {
        let temp = TempDir::new().expect("temp directory");
        let store = store(&temp);
        let source = Uuid::from_u128(4);
        let schema = logical_schema(11, "value");
        let snapshot_id = Uuid::from_u128(1);
        let mut writer = store
            .begin_snapshot(draft(snapshot_id, source, &schema), at(1_700_000_001))
            .expect("begin writer");
        writer
            .append(&envelope(Arc::clone(&schema), source, 0, vec![1]))
            .expect("append");
        let stats = SnapshotStats::try_new(
            writer.row_count,
            writer.stored_byte_count,
            u32::try_from(writer.staged.len()).expect("partition count"),
        )
        .expect("stats");
        let manifest = SnapshotManifest::try_new(
            build_snapshot(&writer.draft, stats).expect("snapshot"),
            writer.staged.clone(),
        )
        .expect("manifest");
        create_final_snapshot_directory(&writer.inner, snapshot_id).expect("final directory");
        writer.installed = true;
        install_partitions(&writer.inner, snapshot_id, &manifest).expect("install files");
        writer.committed = true;
        drop(writer);

        assert!(matches!(
            store.load_manifest(snapshot_id),
            Err(StorageError::NotFound(_))
        ));
        let report = store
            .recover(at(1_700_000_010), Duration::ZERO, 16)
            .expect("recover");
        assert!(report.recovered() >= 1);
        assert!(!final_snapshot_dir(&store.inner, snapshot_id).exists());
        assert!(!staging_snapshot_dir(&store.inner, snapshot_id).exists());

        let committed_id = Uuid::from_u128(2);
        publish(&store, committed_id, source, schema, vec![vec![2]]);
        fs::create_dir(staging_snapshot_dir(&store.inner, committed_id))
            .expect("create postcommit residue");
        store
            .recover(at(1_700_000_020), Duration::ZERO, 16)
            .expect("recover postcommit residue");
        assert!(store.load_manifest(committed_id).is_ok());
        assert!(final_snapshot_dir(&store.inner, committed_id).exists());
        assert!(!staging_snapshot_dir(&store.inner, committed_id).exists());
    }

    #[test]
    fn tombstone_retention_gc_and_activity_are_safe() {
        let temp = TempDir::new().expect("temp directory");
        let store = store(&temp);
        let source = Uuid::from_u128(4);
        let schema = logical_schema(11, "value");
        let snapshot_id = Uuid::from_u128(1);
        publish(
            &store,
            snapshot_id,
            source,
            Arc::clone(&schema),
            vec![vec![1]],
        );
        let reader = store.read_batches(snapshot_id).expect("active reader");
        store
            .tombstone_snapshot(snapshot_id, at(1_700_000_100))
            .expect("tombstone");
        assert!(matches!(
            store.load_manifest(snapshot_id),
            Err(StorageError::NotFound(_))
        ));
        assert!(matches!(
            store.collect_garbage(at(1_700_000_200), Duration::ZERO, 16),
            Err(StorageError::Busy(_))
        ));
        drop(reader);

        let young = store
            .collect_garbage(at(1_700_000_200), Duration::from_secs(200), 16)
            .expect("retain young tombstone");
        assert_eq!(young.deleted(), 0);
        assert!(final_snapshot_dir(&store.inner, snapshot_id).exists());

        let collected = store
            .collect_garbage(at(1_700_001_000), Duration::from_secs(200), 16)
            .expect("collect old tombstone");
        assert_eq!(collected.deleted(), 1);
        assert!(!final_snapshot_dir(&store.inner, snapshot_id).exists());
        let connection = open_connection(&store.inner).expect("open database");
        let count: i64 = connection
            .query_row(
                "SELECT count(*) FROM snapshots WHERE id = ?1",
                params![snapshot_id.to_string()],
                |row| row.get(0),
            )
            .expect("count manifests");
        assert_eq!(count, 0);

        let visible_id = Uuid::from_u128(2);
        publish(&store, visible_id, source, schema, vec![vec![2]]);
        let report = store
            .collect_garbage(at(1_800_000_000), Duration::from_secs(0), 16)
            .expect("ignore visible snapshot");
        assert_eq!(report.deleted(), 0);
        assert!(store.load_manifest(visible_id).is_ok());
    }

    #[test]
    fn configured_activity_limits_fail_fast() {
        let temp = TempDir::new().expect("temp directory");
        let limits = StorageLimits::try_new(8, 8, 100, 1_000_000, 1, 1).expect("activity limits");
        let store = SnapshotStore::open(temp.path(), limits).expect("open store");
        let source = Uuid::from_u128(4);
        let schema = logical_schema(11, "value");
        let snapshot_id = Uuid::from_u128(1);
        publish(
            &store,
            snapshot_id,
            source,
            Arc::clone(&schema),
            vec![vec![1]],
        );
        let reader = store.read_batches(snapshot_id).expect("first reader");
        assert!(matches!(
            store.read_batches(snapshot_id),
            Err(StorageError::Busy(_))
        ));
        drop(reader);

        let writer = store
            .begin_snapshot(
                draft(Uuid::from_u128(2), source, &schema),
                at(1_700_000_001),
            )
            .expect("first publisher");
        assert!(matches!(
            store.begin_snapshot(
                draft(Uuid::from_u128(3), source, &schema),
                at(1_700_000_001)
            ),
            Err(StorageError::Busy(_))
        ));
        drop(writer);
    }

    #[test]
    fn timestamp_and_limit_validation_are_explicit() {
        assert!(StorageLimits::try_new(0, 1, 1, 1, 1, 1).is_err());
        assert!(StorageLimits::try_new(crate::MAX_INPUT_ENVELOPES + 1, 1, 1, 1, 1, 1).is_err());

        let temp = TempDir::new().expect("temp directory");
        let store = store(&temp);
        let source = Uuid::from_u128(4);
        let schema = logical_schema(11, "value");
        assert!(matches!(
            store.begin_snapshot(
                draft(Uuid::from_u128(1), source, &schema),
                at(1_699_999_999)
            ),
            Err(StorageError::InvalidTimestampOrder(_))
        ));
        publish(&store, Uuid::from_u128(2), source, schema, vec![vec![1]]);
        assert!(matches!(
            store.tombstone_snapshot(Uuid::from_u128(2), at(1_699_999_999)),
            Err(StorageError::InvalidTimestampOrder(_))
        ));
        assert!(store
            .recover(
                at(1_700_000_000),
                Duration::ZERO,
                MAX_MAINTENANCE_CANDIDATES + 1,
            )
            .is_err());
    }
}
