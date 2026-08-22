//! Temporary SQLite deduplication index with the lock-first open, close,
//! and recovery protocol of contract section 9.
//!
//! The index is per-run, disposable, and never a persisted dataset. Active
//! ownership is the exclusive OS file lock on `dedup_{run_id}.lock`; the
//! SQLite lease row is advisory recovery metadata. No open path ever deletes
//! a pre-existing file: stale files are removed only by storage recovery
//! under the maintenance gate.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::path::PathBuf;

use chrono::{DateTime, SecondsFormat, Utc};
use fs2::FileExt;
use rusqlite::{params, Connection, OptionalExtension};
use uuid::Uuid;

use crate::{StorageError, StoreInner, MAX_SNAPSHOT_ROWS};

/// SQLite page size frozen by contract section 9.4.
pub const DEDUP_PAGE_SIZE_BYTES: i64 = 4_096;
/// `PRAGMA max_page_count` ceiling: 2,097,152 pages (8 GiB).
pub const MAX_DEDUP_INDEX_PAGES: u32 = 2_097_152;
/// Soft SQLite page-cache target in KiB (`PRAGMA cache_size = -512`).
pub const MAX_DEDUP_INDEX_CACHE_KIB: i64 = 512;
/// Encoded composite dedup key ceiling enforced before any SQLite write
/// (contract 6.1/9).
pub const MAX_DEDUP_KEY_BYTES: usize = 64 * 1024;
/// Disk ceiling per run (contract 9.4).
pub const MAX_DEDUP_INDEX_DISK_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// Deterministic concurrency-test hook parked between `.lock` creation and
/// flock. Test-only; always `None` in production builds.
#[cfg(test)]
pub(crate) static PRE_FLOCK_HOOK: std::sync::Mutex<Option<std::sync::Arc<dyn Fn() + Send + Sync>>> =
    std::sync::Mutex::new(None);

/// Typed result of the exact keep-first insert decision (contract 9.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DedupInsert {
    Inserted { first_source_row_ordinal: u64 },
    Duplicate { first_source_row_ordinal: u64 },
}

/// Owned handle for one run's temporary dedup index.
pub struct DedupIndex {
    connection: Option<Connection>,
    lock_file: Option<File>,
    sqlite_path: PathBuf,
    lock_path: PathBuf,
    inserted_rows: Cell<u64>,
}

impl DedupIndex {
    /// Decides keep-first via SQLite BLOB primary-key equality over the full
    /// canonical key bytes. `changes() == 1` means first occurrence; zero
    /// means duplicate and the stored first ordinal is read back. No hash
    /// participates in the decision (contract 9.2).
    pub fn insert_first(
        &self,
        node_id: Uuid,
        rule_ordinal: u32,
        key_bytes: &[u8],
        current_source_row_ordinal: u64,
    ) -> Result<DedupInsert, StorageError> {
        if key_bytes.is_empty() {
            return Err(StorageError::InvalidDraft(
                "dedup key bytes must not be empty",
            ));
        }
        if key_bytes.len() > MAX_DEDUP_KEY_BYTES {
            return Err(StorageError::DedupKeyLimitExceeded {
                actual: key_bytes.len(),
                maximum: MAX_DEDUP_KEY_BYTES,
            });
        }
        if self.inserted_rows.get() >= MAX_SNAPSHOT_ROWS {
            return Err(StorageError::RowLimitExceeded {
                actual: self.inserted_rows.get(),
                maximum: MAX_SNAPSHOT_ROWS,
            });
        }
        let current = checked_i64(current_source_row_ordinal)?;
        let connection = self
            .connection
            .as_ref()
            .ok_or(StorageError::InvalidDraft("dedup index is already closed"))?;
        let inserted = connection
            .execute(
                "INSERT INTO dedup_index(node_id, rule_ordinal, key_bytes,
                                        first_source_row_ordinal)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT DO NOTHING",
                params![
                    node_id.as_bytes(),
                    i64::from(rule_ordinal),
                    key_bytes,
                    current
                ],
            )
            .map_err(|error| classified_sqlite_error("insert dedup key", error))?;
        if inserted == 1 {
            let updated = self
                .inserted_rows
                .get()
                .checked_add(1)
                .ok_or(StorageError::ArithmeticOverflow("dedup index row count"))?;
            self.inserted_rows.set(updated);
            return Ok(DedupInsert::Inserted {
                first_source_row_ordinal: current_source_row_ordinal,
            });
        }
        let existing: i64 = connection
            .query_row(
                "SELECT first_source_row_ordinal FROM dedup_index
                 WHERE node_id = ?1 AND rule_ordinal = ?2 AND key_bytes = ?3",
                params![node_id.as_bytes(), i64::from(rule_ordinal), key_bytes],
                |row| row.get(0),
            )
            .optional()
            .map_err(|_| StorageError::database("read dedup first ordinal"))?
            .ok_or(StorageError::InvalidManifest(
                "dedup duplicate lost its first-seen row",
            ))?;
        Ok(DedupInsert::Duplicate {
            first_source_row_ordinal: checked_u64(existing)?,
        })
    }

    /// Explicit cleanup: releases the `.lock`, closes SQLite, deletes both
    /// files, and fails closed if deletion fails (contract 9.3).
    pub fn close_and_delete(mut self) -> Result<(), StorageError> {
        self.finish()
    }

    fn finish(&mut self) -> Result<(), StorageError> {
        // Contract order: release the lock, close SQLite, delete both files.
        if let Some(lock_file) = self.lock_file.take() {
            let _ = fs2::FileExt::unlock(&lock_file);
            drop(lock_file);
        }
        if let Some(connection) = self.connection.take() {
            connection
                .close()
                .map_err(|_| StorageError::database("close dedup index"))?;
        }
        remove_dedup_file(&self.sqlite_path)?;
        remove_dedup_file(&self.lock_path)
    }
}

impl Drop for DedupIndex {
    /// Defense-in-depth best-effort cleanup; never a substitute for
    /// `close_and_delete` (contract 9.3).
    fn drop(&mut self) {
        let _ = self.finish();
    }
}

fn checked_i64(value: u64) -> Result<i64, StorageError> {
    i64::try_from(value).map_err(|_| StorageError::ArithmeticOverflow("dedup row ordinal"))
}

fn checked_u64(value: i64) -> Result<u64, StorageError> {
    u64::try_from(value).map_err(|_| StorageError::InvalidManifest("dedup row ordinal"))
}

fn remove_dedup_file(path: &std::path::Path) -> Result<(), StorageError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(StorageError::InvalidManifest(
            "dedup index path must not be a symlink",
        )),
        Ok(_) => fs::remove_file(path)
            .map_err(|error| StorageError::io("delete dedup index file", &error)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(StorageError::io("inspect dedup index file", &error)),
    }
}

fn reject_dedup_symlink(path: &std::path::Path) -> Result<(), StorageError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(StorageError::InvalidManifest(
            "dedup index path must not be a symlink",
        )),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(StorageError::io("inspect dedup index path", &error)),
    }
}

pub(crate) fn dedup_temp_root(inner: &StoreInner) -> PathBuf {
    inner.root.join("temp")
}

pub(crate) fn dedup_sqlite_path(inner: &StoreInner, run_id: Uuid) -> PathBuf {
    dedup_temp_root(inner).join(format!("dedup_{run_id}.sqlite"))
}

pub(crate) fn dedup_lock_path(inner: &StoreInner, run_id: Uuid) -> PathBuf {
    dedup_temp_root(inner).join(format!("dedup_{run_id}.lock"))
}

/// Opens one run's exclusive dedup index following the lock-first protocol
/// of contract section 9.1.
pub(crate) fn open_dedup_index(
    inner: &StoreInner,
    run_id: Uuid,
    bundle_id: Uuid,
    started_at: DateTime<Utc>,
) -> Result<DedupIndex, StorageError> {
    if run_id.is_nil() || bundle_id.is_nil() {
        return Err(StorageError::InvalidDraft(
            "dedup index identities must not be nil",
        ));
    }
    let temp_root = dedup_temp_root(inner);
    crate::ensure_private_directory(&temp_root)?;

    let sqlite_path = dedup_sqlite_path(inner, run_id);
    let lock_path = dedup_lock_path(inner, run_id);
    reject_dedup_symlink(&sqlite_path)?;
    reject_dedup_symlink(&lock_path)?;

    let mut sqlite_created_by_attempt = false;
    let mut lock_created_by_attempt = false;
    let result = open_dedup_index_inner(
        run_id,
        bundle_id,
        started_at,
        &sqlite_path,
        &lock_path,
        MAX_DEDUP_INDEX_PAGES,
        &mut sqlite_created_by_attempt,
        &mut lock_created_by_attempt,
    );
    match result {
        Ok(index) => Ok(index),
        Err(error) => {
            // Roll back only files created by this attempt; a pre-existing
            // `.sqlite` or `.lock` is never deleted (contract 9.1).
            if sqlite_created_by_attempt {
                let _ = fs::remove_file(&sqlite_path);
            }
            if lock_created_by_attempt {
                let _ = fs::remove_file(&lock_path);
            }
            Err(error)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn open_dedup_index_inner(
    run_id: Uuid,
    bundle_id: Uuid,
    started_at: DateTime<Utc>,
    sqlite_path: &std::path::Path,
    lock_path: &std::path::Path,
    max_page_count: u32,
    sqlite_created_by_attempt: &mut bool,
    lock_created_by_attempt: &mut bool,
) -> Result<DedupIndex, StorageError> {
    // Step 1: exclusively create the lock path and acquire the OS lock.
    let lock_file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(lock_path)
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                StorageError::AlreadyExists(run_id)
            } else {
                StorageError::io("create dedup lock file", &error)
            }
        })?;
    *lock_created_by_attempt = true;
    set_owner_only_permissions(lock_path);
    // Deterministic test hook: parks the caller inside the open critical
    // section between `.lock` creation and flock so concurrency tests can
    // synchronize on the exact window without timing.
    #[cfg(test)]
    {
        // Clone the hook out and release the mutex before invoking it so a
        // hook that synchronizes with the driving test cannot self-deadlock.
        let hook = PRE_FLOCK_HOOK
            .lock()
            .expect("pre-flock hook mutex")
            .as_ref()
            .cloned();
        if let Some(hook) = hook {
            hook();
        }
    }
    lock_file.try_lock_exclusive().map_err(|error| {
        if error.kind() == std::io::ErrorKind::WouldBlock {
            StorageError::Busy("dedup lock is held by an active run")
        } else {
            StorageError::io("acquire dedup lock", &error)
        }
    })?;
    // Lock-identity revalidation (E4-S1-R1 blocker D): a concurrent recovery
    // pass must never have unlinked or replaced the lock between creation and
    // acquisition. A live index may only be returned while the flock it holds
    // still refers to the on-disk `.lock` (contract 9.1 step 3 / 9.4).
    if !lock_file_identity_matches(&lock_file, lock_path) {
        return Err(StorageError::Busy("dedup lock file lost during open"));
    }

    // Step 2: while holding the lock, exclusively create the SQLite path.
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(sqlite_path)
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                StorageError::AlreadyExists(run_id)
            } else {
                StorageError::io("create dedup database file", &error)
            }
        })?;
    *sqlite_created_by_attempt = true;
    set_owner_only_permissions(sqlite_path);

    // Step 3: open SQLite and apply every PRAGMA before any table or lease
    // row exists, so the frozen page size applies to the new database.
    let connection =
        Connection::open(sqlite_path).map_err(|_| StorageError::database("open dedup index"))?;
    connection
        .execute_batch(&format!(
            "PRAGMA page_size = {DEDUP_PAGE_SIZE_BYTES};
             PRAGMA max_page_count = {max_page_count};
             PRAGMA cache_size = -{MAX_DEDUP_INDEX_CACHE_KIB};
             PRAGMA journal_mode = DELETE;"
        ))
        .map_err(|_| StorageError::database("configure dedup index pragmas"))?;
    connection
        .execute_batch(
            "CREATE TABLE dedup_index (
                 node_id                   BLOB    NOT NULL,
                 rule_ordinal              INTEGER NOT NULL,
                 key_bytes                 BLOB    NOT NULL,
                 first_source_row_ordinal  INTEGER NOT NULL,
                 PRIMARY KEY (node_id, rule_ordinal, key_bytes)
             ) WITHOUT ROWID;

             CREATE TABLE dedup_lease (
                 run_id          BLOB NOT NULL PRIMARY KEY,
                 bundle_id       BLOB NOT NULL,
                 started_at_utc  TEXT NOT NULL
             ) WITHOUT ROWID;",
        )
        .map_err(|_| StorageError::database("initialize dedup index tables"))?;
    connection
        .execute(
            "INSERT INTO dedup_lease(run_id, bundle_id, started_at_utc) VALUES (?1, ?2, ?3)",
            params![
                run_id.as_bytes(),
                bundle_id.as_bytes(),
                started_at.to_rfc3339_opts(SecondsFormat::Nanos, true)
            ],
        )
        .map_err(|_| StorageError::database("write dedup lease"))?;

    Ok(DedupIndex {
        connection: Some(connection),
        lock_file: Some(lock_file),
        sqlite_path: sqlite_path.to_path_buf(),
        lock_path: lock_path.to_path_buf(),
        inserted_rows: Cell::new(0),
    })
}

#[cfg(unix)]
fn set_owner_only_permissions(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;
    let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o600));
}

/// Confirms the flocked file description still refers to the current on-disk
/// `.lock`: same inode and device, and the path still exists with at least
/// one link. Detects the recovery/open TOCTOU where an unlinked inode was
/// locked after the directory entry disappeared.
#[cfg(unix)]
fn lock_file_identity_matches(file: &std::fs::File, path: &std::path::Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    let Ok(fd_metadata) = file.metadata() else {
        return false;
    };
    let Ok(path_metadata) = fs::metadata(path) else {
        return false;
    };
    fd_metadata.ino() == path_metadata.ino()
        && fd_metadata.dev() == path_metadata.dev()
        && path_metadata.nlink() > 0
}

/// Platforms without Unix inodes rely on the private 0700 temp root plus the
/// maintenance gate; no unlink-during-open window exists there because only
/// this crate deletes these files, always under the gate.
#[cfg(not(unix))]
fn lock_file_identity_matches(_file: &std::fs::File, _path: &std::path::Path) -> bool {
    true
}

/// Maps SQLite page-cap exhaustion (SQLITE_FULL) to a typed bounded limit so
/// callers can classify it instead of an unrelated internal fault.
fn classified_sqlite_error(operation: &'static str, error: rusqlite::Error) -> StorageError {
    if let rusqlite::Error::SqliteFailure(failure, _) = &error {
        if failure.code == rusqlite::ffi::ErrorCode::DiskFull {
            return StorageError::DedupIndexLimitExceeded {
                resource: "page",
                maximum: u64::from(MAX_DEDUP_INDEX_PAGES),
            };
        }
    }
    StorageError::database(operation)
}

#[cfg(not(unix))]
fn set_owner_only_permissions(_path: &std::path::Path) {
    // Platforms without Unix modes rely on the strongest owner-only ACL the
    // platform provides by default for newly created files in a 0700-equivalent
    // private directory; recorded here per contract section 9.1.
}

/// Recovery pass over the union of `dedup_*.sqlite` and `dedup_*.lock`
/// candidates (contract 9.3). Runs under the maintenance gate. A candidate is
/// stale only when its lock is absent or acquirable; an active run keeps both
/// files.
pub(crate) fn recover_dedup_candidates(
    inner: &StoreInner,
    maximum: u32,
    report: &mut crate::RecoveryReport,
) -> Result<(), StorageError> {
    let temp_root = dedup_temp_root(inner);
    let entries = match fs::read_dir(&temp_root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(StorageError::io("scan dedup temp root", &error)),
    };

    let mut candidates: BTreeMap<Uuid, (bool, bool)> = BTreeMap::new();
    for entry in entries {
        if candidates.len() >= maximum as usize {
            break;
        }
        let entry = entry.map_err(|error| StorageError::io("read dedup temp entry", &error))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let candidate = parse_dedup_candidate(name);
        let Some((run_id, is_sqlite)) = candidate else {
            continue;
        };
        let entry_state = candidates.entry(run_id).or_insert((false, false));
        if is_sqlite {
            entry_state.0 = true;
        } else {
            entry_state.1 = true;
        }
    }

    for (run_id, (has_sqlite, has_lock)) in candidates {
        report.examined = report.examined.saturating_add(1);
        if !has_sqlite && !has_lock {
            continue;
        }
        if has_lock && dedup_lock_is_active(inner, run_id)? {
            // Active run: neither file is removed.
            continue;
        }
        if has_sqlite {
            remove_dedup_file(&dedup_sqlite_path(inner, run_id))?;
        }
        if has_lock {
            remove_dedup_file(&dedup_lock_path(inner, run_id))?;
        }
        report.recovered = report.recovered.saturating_add(1);
    }
    Ok(())
}

fn parse_dedup_candidate(name: &str) -> Option<(Uuid, bool)> {
    let rest = name.strip_prefix("dedup_")?;
    let (run_id_text, is_sqlite) = rest.strip_suffix(".sqlite").map_or_else(
        || rest.strip_suffix(".lock").map(|stem| (stem, false)),
        |stem| Some((stem, true)),
    )?;
    let run_id = Uuid::parse_str(run_id_text).ok()?;
    Some((run_id, is_sqlite))
}

/// Returns `true` when the lock exists and is held by an active owner.
fn dedup_lock_is_active(inner: &StoreInner, run_id: Uuid) -> Result<bool, StorageError> {
    let lock_path = dedup_lock_path(inner, run_id);
    let lock_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&lock_path)
        .map_err(|error| StorageError::io("open stale dedup lock", &error))?;
    match lock_file.try_lock_exclusive() {
        Ok(()) => {
            // Acquisition succeeded: the candidate is stale. Release again;
            // removal happens by the caller.
            let _ = fs2::FileExt::unlock(&lock_file);
            Ok(false)
        }
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(true),
        Err(error) => Err(StorageError::io("probe stale dedup lock", &error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RecoveryReport, StorageLimits};
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;

    fn at(second: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(second, 0).expect("valid timestamp")
    }

    fn store(temp: &TempDir) -> crate::SnapshotStore {
        crate::SnapshotStore::open(temp.path(), StorageLimits::default()).expect("store")
    }

    fn key(bytes: usize) -> Vec<u8> {
        (0..bytes).map(|index| (index % 251) as u8).collect()
    }

    #[test]
    fn insert_first_decides_keep_first_by_exact_key_identity() {
        let temp = TempDir::new().expect("temp");
        let store = store(&temp);
        let run_id = Uuid::from_u128(0xD001);
        let index = store
            .open_dedup_index(run_id, Uuid::from_u128(0xD002), at(1_700_000_000))
            .expect("index");
        let node = Uuid::from_u128(0xD003);

        let first = index
            .insert_first(node, 0, &key(16), 5)
            .expect("first insert");
        assert_eq!(
            first,
            DedupInsert::Inserted {
                first_source_row_ordinal: 5
            }
        );
        let duplicate = index
            .insert_first(node, 0, &key(16), 9)
            .expect("duplicate insert");
        assert_eq!(
            duplicate,
            DedupInsert::Duplicate {
                first_source_row_ordinal: 5
            }
        );
        // Different rule ordinal or node id means a different namespace.
        let other_rule = index
            .insert_first(node, 1, &key(16), 11)
            .expect("other rule");
        assert!(matches!(other_rule, DedupInsert::Inserted { .. }));
        let other_node = index
            .insert_first(Uuid::from_u128(0xD004), 0, &key(16), 12)
            .expect("other node");
        assert!(matches!(other_node, DedupInsert::Inserted { .. }));
        // Byte-level identity, not hash identity: a one-byte change is new.
        let mut mutated = key(16);
        mutated[0] ^= 0xFF;
        let mutated = index
            .insert_first(node, 0, &mutated, 13)
            .expect("mutated key");
        assert!(matches!(mutated, DedupInsert::Inserted { .. }));

        index.close_and_delete().expect("close");
    }

    #[test]
    fn key_limits_fail_closed_before_any_write() {
        let temp = TempDir::new().expect("temp");
        let store = store(&temp);
        let index = store
            .open_dedup_index(Uuid::from_u128(0xD010), Uuid::from_u128(0xD011), at(1))
            .expect("index");
        assert!(matches!(
            index.insert_first(Uuid::from_u128(1), 0, &[], 0),
            Err(StorageError::InvalidDraft(_))
        ));
        let oversized = MAX_DEDUP_KEY_BYTES + 1;
        assert!(matches!(
            index.insert_first(Uuid::from_u128(1), 0, &key(oversized), 0),
            Err(StorageError::DedupKeyLimitExceeded {
                actual: _,
                maximum: MAX_DEDUP_KEY_BYTES,
            })
        ));
        index.close_and_delete().expect("close");
    }

    #[test]
    fn pre_existing_files_are_never_deleted_and_fail_closed() {
        let temp = TempDir::new().expect("temp");
        let store = store(&temp);
        let run_id = Uuid::from_u128(0xD020);
        let sqlite_path = dedup_sqlite_path(&store.inner, run_id);
        let lock_path = dedup_lock_path(&store.inner, run_id);
        std::fs::write(&sqlite_path, b"pre-existing").expect("pre-existing sqlite");
        std::fs::write(&lock_path, b"").expect("pre-existing lock");

        assert!(matches!(
            store.open_dedup_index(run_id, Uuid::from_u128(1), at(1)),
            Err(StorageError::AlreadyExists(id)) if id == run_id
        ));
        assert_eq!(
            std::fs::read(&sqlite_path).expect("sqlite untouched"),
            b"pre-existing"
        );
        assert!(lock_path.exists(), "pre-existing lock untouched");

        // Only the SQLite file pre-exists: the attempt creates and acquires
        // the lock, then rolls the lock back because it created it.
        std::fs::remove_file(&lock_path).expect("remove lock");
        assert!(matches!(
            store.open_dedup_index(run_id, Uuid::from_u128(1), at(1)),
            Err(StorageError::AlreadyExists(id)) if id == run_id
        ));
        assert_eq!(
            std::fs::read(&sqlite_path).expect("sqlite untouched again"),
            b"pre-existing"
        );
        assert!(!lock_path.exists(), "attempt-created lock rolled back");
    }

    #[test]
    fn close_and_delete_removes_both_files_and_allows_reopen() {
        let temp = TempDir::new().expect("temp");
        let store = store(&temp);
        let run_id = Uuid::from_u128(0xD030);
        let index = store
            .open_dedup_index(run_id, Uuid::from_u128(1), at(1))
            .expect("index");
        let sqlite_path = dedup_sqlite_path(&store.inner, run_id);
        let lock_path = dedup_lock_path(&store.inner, run_id);
        index.close_and_delete().expect("close");
        assert!(!sqlite_path.exists());
        assert!(!lock_path.exists());
        let reopened = store
            .open_dedup_index(run_id, Uuid::from_u128(1), at(2))
            .expect("reopen");
        reopened.close_and_delete().expect("close again");
    }

    #[test]
    fn drop_is_best_effort_cleanup() {
        let temp = TempDir::new().expect("temp");
        let store = store(&temp);
        let run_id = Uuid::from_u128(0xD040);
        {
            let index = store
                .open_dedup_index(run_id, Uuid::from_u128(1), at(1))
                .expect("index");
            index
                .insert_first(Uuid::from_u128(2), 0, &key(8), 1)
                .expect("insert");
            // Drop without close_and_delete.
        }
        assert!(
            !dedup_sqlite_path(&store.inner, run_id).exists(),
            "drop removes the sqlite file"
        );
        assert!(!dedup_lock_path(&store.inner, run_id).exists());
    }

    #[test]
    fn recovery_removes_stale_and_orphan_candidates_and_keeps_active_ones() {
        let temp = TempDir::new().expect("temp");
        let store = store(&temp);

        // Stale pair: lock exists but is free.
        let stale = Uuid::from_u128(0xD050);
        std::fs::write(dedup_sqlite_path(&store.inner, stale), b"stale").expect("sqlite");
        std::fs::write(dedup_lock_path(&store.inner, stale), b"").expect("lock");

        // Active pair: lock is held by an open file description.
        let active = Uuid::from_u128(0xD051);
        std::fs::write(dedup_sqlite_path(&store.inner, active), b"active").expect("sqlite");
        let held = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(dedup_lock_path(&store.inner, active))
            .expect("lock file");
        fs2::FileExt::try_lock_exclusive(&held).expect("acquire active lock");

        // Orphan sqlite without a lock.
        let orphan = Uuid::from_u128(0xD052);
        std::fs::write(dedup_sqlite_path(&store.inner, orphan), b"orphan").expect("sqlite");

        // Unrelated files are ignored.
        std::fs::write(dedup_temp_root(&store.inner).join("unrelated.txt"), b"x")
            .expect("unrelated");

        let mut report = RecoveryReport::default();
        recover_dedup_candidates(&store.inner, crate::MAX_MAINTENANCE_CANDIDATES, &mut report)
            .expect("recovery");
        assert_eq!(report.recovered(), 2, "stale pair plus orphan removed");
        assert!(!dedup_sqlite_path(&store.inner, stale).exists());
        assert!(!dedup_lock_path(&store.inner, stale).exists());
        assert!(!dedup_sqlite_path(&store.inner, orphan).exists());
        assert!(
            dedup_sqlite_path(&store.inner, active).exists(),
            "active kept"
        );
        assert!(
            dedup_lock_path(&store.inner, active).exists(),
            "active kept"
        );
        assert!(
            dedup_temp_root(&store.inner).join("unrelated.txt").exists(),
            "unrelated file untouched"
        );
        drop(held);
    }

    #[test]
    fn page_cap_fails_closed_with_a_bounded_error() {
        let temp = TempDir::new().expect("temp");
        let store = store(&temp);
        let run_id = Uuid::from_u128(0xD060);
        let index = store
            .open_dedup_index(run_id, Uuid::from_u128(1), at(1))
            .expect("index");
        // Test-side instrumentation of the frozen page ceiling (V13).
        {
            let connection = index.connection.as_ref().expect("connection");
            connection
                .execute_batch("PRAGMA max_page_count = 8;")
                .expect("shrink page cap");
        }
        let node = Uuid::from_u128(2);
        let mut hit_limit = false;
        for ordinal in 0..20_000_u64 {
            let mut bytes = key(64);
            bytes[0..8].copy_from_slice(&ordinal.to_le_bytes());
            match index.insert_first(node, 0, &bytes, ordinal) {
                Ok(_) => {}
                Err(StorageError::RowLimitExceeded { .. }) => {}
                Err(StorageError::DedupIndexLimitExceeded {
                    resource: "page",
                    maximum,
                }) => {
                    assert_eq!(maximum, u64::from(MAX_DEDUP_INDEX_PAGES));
                    hit_limit = true;
                    break;
                }
                Err(error) => panic!("unexpected error: {error:?}"),
            }
        }
        assert!(hit_limit, "the page cap must stop unbounded growth");
        index.close_and_delete().expect("close");
    }

    /// The frozen page ceiling is set verbatim on every opened index.
    #[test]
    fn max_page_count_pragma_matches_the_frozen_constant() {
        let temp = TempDir::new().expect("temp");
        let store = store(&temp);
        let index = store
            .open_dedup_index(Uuid::from_u128(0xD061), Uuid::from_u128(1), at(1))
            .expect("index");
        let configured: i64 = index
            .connection
            .as_ref()
            .expect("connection")
            .query_row("PRAGMA max_page_count", [], |row| row.get(0))
            .expect("pragma");
        assert_eq!(configured, i64::from(MAX_DEDUP_INDEX_PAGES));
        index.close_and_delete().expect("close");
    }

    /// A lone pre-existing `.lock` (crash points 1–2 of section 9.1) fails
    /// closed with `AlreadyExists` and the file survives byte-identical.
    #[test]
    fn lone_lock_open_path_fails_closed_and_preserves_the_file() {
        let temp = TempDir::new().expect("temp");
        let store = store(&temp);
        let run_id = Uuid::from_u128(0xD062);
        let lock_path = dedup_lock_path(&store.inner, run_id);
        std::fs::write(&lock_path, b"prior-attempt").expect("seed lock");
        assert!(matches!(
            store.open_dedup_index(run_id, Uuid::from_u128(1), at(1)),
            Err(StorageError::AlreadyExists(id)) if id == run_id
        ));
        assert_eq!(
            std::fs::read(&lock_path).expect("lock contents"),
            b"prior-attempt"
        );
        assert!(!dedup_sqlite_path(&store.inner, run_id).exists());
    }

    /// Recovery removes a lone free `.lock` and keeps a held one — both
    /// branches of the union-scan leaf that earlier builds left unpinned.
    #[test]
    fn recovery_handles_lone_lock_candidates_by_liveness() {
        let temp = TempDir::new().expect("temp");
        let store = store(&temp);

        let free = Uuid::from_u128(0xD063);
        std::fs::write(dedup_lock_path(&store.inner, free), b"").expect("free lock");

        let held = Uuid::from_u128(0xD064);
        let held_file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(dedup_lock_path(&store.inner, held))
            .expect("held lock file");
        fs2::FileExt::try_lock_exclusive(&held_file).expect("hold lock");

        let mut report = RecoveryReport::default();
        recover_dedup_candidates(&store.inner, crate::MAX_MAINTENANCE_CANDIDATES, &mut report)
            .expect("recover");
        assert!(
            !dedup_lock_path(&store.inner, free).exists(),
            "free removed"
        );
        assert!(dedup_lock_path(&store.inner, held).exists(), "held kept");
        drop(held_file);
    }

    /// A valid SQLite file stranded without lock or lease (crash between
    /// SQLite-file creation and open/lease) is an orphan candidate: refused
    /// at open with `AlreadyExists`, reclaimed by recovery.
    #[test]
    fn sqlite_without_lock_or_lease_is_refused_then_reclaimed() {
        let temp = TempDir::new().expect("temp");
        let store = store(&temp);
        let run_id = Uuid::from_u128(0xD065);
        let sqlite_path = dedup_sqlite_path(&store.inner, run_id);
        // A real SQLite database without dedup tables or a lease row.
        {
            let connection = rusqlite::Connection::open(&sqlite_path).expect("seed db");
            connection
                .execute_batch("CREATE TABLE marker(x INTEGER);")
                .expect("schema");
        }
        assert!(matches!(
            store.open_dedup_index(run_id, Uuid::from_u128(1), at(1)),
            Err(StorageError::AlreadyExists(_))
        ));
        assert!(
            sqlite_path.exists(),
            "pre-existing db never deleted on open"
        );
        let mut report = RecoveryReport::default();
        recover_dedup_candidates(&store.inner, crate::MAX_MAINTENANCE_CANDIDATES, &mut report)
            .expect("recover");
        assert!(!sqlite_path.exists(), "orphan reclaimed by recovery");
    }

    /// Blocker D, exclusion half: while an opener is parked inside its
    /// critical section (guard held, `.lock` created, flock not yet taken),
    /// the maintenance gate must be unavailable to recovery. Two independent
    /// barriers make this exact: `entered` proves the opener sits inside
    /// `PRE_FLOCK_HOOK` still holding its activity guard; `release` keeps it
    /// parked there until the maintenance-gate probe has been recorded and
    /// any guard it won has been dropped. Barrier synchronization only; no
    /// sleep, timeout, or scheduling assumption.
    #[test]
    fn open_critical_section_excludes_recovery_via_activity_guard() {
        let temp = TempDir::new().expect("temp");
        let store = store(&temp);
        let run_id = Uuid::from_u128(0xD066);

        let entered = Arc::new(std::sync::Barrier::new(2));
        let release = Arc::new(std::sync::Barrier::new(2));
        let hook_entered = Arc::clone(&entered);
        let hook_release = Arc::clone(&release);
        *PRE_FLOCK_HOOK.lock().expect("hook") = Some(Arc::new(move || {
            // Announce arrival inside the open window, then stay parked
            // there until the driving test has probed the maintenance gate.
            hook_entered.wait();
            hook_release.wait();
        }));

        // Every synchronization and cleanup step below precedes the
        // assertions, so even a failing probe releases the opener, clears
        // the hook, and joins the thread instead of hanging or leaking
        // state into later tests.
        let opener_store = store.clone();
        let handle = std::thread::spawn(move || {
            opener_store.open_dedup_index(run_id, Uuid::from_u128(1), at(1))
        });

        entered.wait(); // opener holds the guard between .lock creation and flock
        let gate = crate::acquire_maintenance(&store.inner);
        let busy = matches!(gate, Err(StorageError::Busy(_)));
        drop(gate); // release any guard this probe may have won

        release.wait(); // unpark the opener; it completes its open
        *PRE_FLOCK_HOOK.lock().expect("hook") = None;
        let result = handle.join().expect("opener thread");

        assert!(
            busy,
            "recovery gate must be excluded while the open window is live"
        );
        let index = result.expect("index opens after the window");
        index
            .insert_first(Uuid::from_u128(9), 0, &key(8), 0)
            .expect("insert into freshly opened index");
        index.close_and_delete().expect("close");
        *PRE_FLOCK_HOOK.lock().expect("hook") = None;
    }

    /// Blocker D, liveness half: if the lock file disappears during the open
    /// window (the exact unlink the old race allowed), the opener must fail
    /// closed instead of returning an index whose flock no longer guards any
    /// directory entry.
    #[test]
    fn lost_lock_during_open_fails_closed_without_residue() {
        let temp = TempDir::new().expect("temp");
        let store = store(&temp);
        let run_id = Uuid::from_u128(0xD067);
        let lock_path = dedup_lock_path(&store.inner, run_id);

        let unlink_lock = {
            let lock_path = lock_path.clone();
            move || {
                let _ = std::fs::remove_file(&lock_path);
            }
        };
        *PRE_FLOCK_HOOK.lock().expect("hook") = Some(Arc::new(unlink_lock));

        let opener_store = store.clone();
        let result = opener_store
            .open_dedup_index(run_id, Uuid::from_u128(1), at(1))
            .err()
            .expect("open must fail");
        *PRE_FLOCK_HOOK.lock().expect("hook") = None;

        assert!(
            matches!(result, StorageError::Busy(message) if message.contains("lost")),
            "unlinked lock must fail the open: {result:?}"
        );
        assert!(!dedup_sqlite_path(&store.inner, run_id).exists());
        assert!(!lock_path.exists());
    }

    #[cfg(unix)]
    #[test]
    fn temp_directory_and_database_permissions_are_private() {
        use std::os::unix::fs::PermissionsExt;
        let temp = TempDir::new().expect("temp");
        let store = store(&temp);
        let run_id = Uuid::from_u128(0xD090);
        let _index = store
            .open_dedup_index(run_id, Uuid::from_u128(1), at(1))
            .expect("index");
        let sqlite_mode = std::fs::metadata(dedup_sqlite_path(&store.inner, run_id))
            .expect("sqlite metadata")
            .permissions()
            .mode();
        assert_eq!(sqlite_mode & 0o777, 0o600, "dedup database must be 0600");
        let root_mode = std::fs::metadata(dedup_temp_root(&store.inner))
            .expect("temp root metadata")
            .permissions()
            .mode();
        assert_eq!(root_mode & 0o777, 0o700, "dedup temp root must be 0700");
    }

    #[test]
    fn lease_row_records_the_run_identity() {
        let temp = TempDir::new().expect("temp");
        let store = store(&temp);
        let run_id = Uuid::from_u128(0xD070);
        let bundle_id = Uuid::from_u128(0xD071);
        let index = store
            .open_dedup_index(run_id, bundle_id, at(1_700_000_123))
            .expect("index");
        let connection = index.connection.as_ref().expect("connection");
        let (stored_run, stored_bundle): (Vec<u8>, Vec<u8>) = connection
            .query_row(
                "SELECT run_id, bundle_id FROM dedup_lease WHERE run_id = ?1",
                [run_id.as_bytes()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("lease row");
        assert_eq!(stored_run, run_id.as_bytes().to_vec());
        assert_eq!(stored_bundle, bundle_id.as_bytes().to_vec());
        index.close_and_delete().expect("close");
    }

    #[test]
    fn snapshot_recovery_pass_covers_dedup_candidates() {
        let temp = TempDir::new().expect("temp");
        let store = store(&temp);
        let stale = Uuid::from_u128(0xD080);
        std::fs::write(dedup_sqlite_path(&store.inner, stale), b"stale").expect("sqlite");
        std::fs::write(dedup_lock_path(&store.inner, stale), b"").expect("lock");
        let report = store
            .recover(
                at(1_700_000_000),
                Duration::ZERO,
                crate::MAX_MAINTENANCE_CANDIDATES,
            )
            .expect("recover");
        assert!(
            report.recovered() >= 1,
            "maintenance recovery must include dedup candidates"
        );
        assert!(!dedup_sqlite_path(&store.inner, stale).exists());
    }
}
