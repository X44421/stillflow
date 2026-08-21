//! Experimental exclusive SQLite exact-dedup index.
//!
//! Probe for Issue #54 section 9. Not an approved merge surface.

use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use fs2::FileExt;
use rusqlite::{params, Connection};
use uuid::Uuid;

use crate::store::{format_timestamp, StoreInner};
use crate::StorageError;

pub const MAX_DEDUP_INDEX_PAGES: u32 = 2_097_152;
pub const MAX_DEDUP_INDEX_CACHE_BYTES: usize = 512 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DedupInsert {
    Inserted { first_source_row_ordinal: u64 },
    Duplicate { first_source_row_ordinal: u64 },
}

pub struct DedupIndex {
    connection: Option<Connection>,
    lock_file: Option<File>,
    sqlite_path: PathBuf,
    lock_path: PathBuf,
    created_sqlite: bool,
    created_lock: bool,
    closed: bool,
}

impl DedupIndex {
    pub(crate) fn open(
        inner: &StoreInner,
        run_id: Uuid,
        bundle_id: Uuid,
        started_at: DateTime<Utc>,
    ) -> Result<Self, StorageError> {
        if run_id.is_nil() || bundle_id.is_nil() {
            return Err(StorageError::InvalidDraft(
                "dedup index identities must not be nil",
            ));
        }
        let temp_root = inner.root.join("tmp");
        create_temp_dir(&temp_root)?;
        let lock_path = temp_root.join(format!("dedup_{run_id}.lock"));
        let sqlite_path = temp_root.join(format!("dedup_{run_id}.sqlite"));

        let mut created_lock = false;
        let mut created_sqlite = false;
        let result = open_inner(
            &lock_path,
            &sqlite_path,
            run_id,
            bundle_id,
            started_at,
            &mut created_lock,
            &mut created_sqlite,
        );
        match result {
            Ok(index) => Ok(index),
            Err(error) => {
                rollback_created(&lock_path, created_lock, &sqlite_path, created_sqlite);
                Err(error)
            }
        }
    }

    pub fn insert_first(
        &self,
        node_id: Uuid,
        rule_ordinal: u32,
        key_bytes: &[u8],
        current_source_row_ordinal: u64,
    ) -> Result<DedupInsert, StorageError> {
        let ordinal = i64::try_from(current_source_row_ordinal)
            .map_err(|_| StorageError::ArithmeticOverflow("source row ordinal"))?;
        let rule_ordinal = i64::from(rule_ordinal);
        let connection = self
            .connection
            .as_ref()
            .ok_or(StorageError::InvalidDraft("dedup index is closed"))?;
        connection
            .execute(
                "INSERT INTO dedup_index(
                     node_id, rule_ordinal, key_bytes, first_source_row_ordinal
                 ) VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT DO NOTHING",
                params![
                    node_id.as_bytes().as_slice(),
                    rule_ordinal,
                    key_bytes,
                    ordinal
                ],
            )
            .map_err(|_| StorageError::database("insert dedup key"))?;
        if connection.changes() == 1 {
            return Ok(DedupInsert::Inserted {
                first_source_row_ordinal: current_source_row_ordinal,
            });
        }
        let first: i64 = connection
            .query_row(
                "SELECT first_source_row_ordinal
                 FROM dedup_index
                 WHERE node_id = ?1 AND rule_ordinal = ?2 AND key_bytes = ?3",
                params![node_id.as_bytes().as_slice(), rule_ordinal, key_bytes],
                |row| row.get(0),
            )
            .map_err(|_| StorageError::database("load first-seen dedup ordinal"))?;
        let first_source_row_ordinal = u64::try_from(first)
            .map_err(|_| StorageError::ArithmeticOverflow("first-seen source row ordinal"))?;
        Ok(DedupInsert::Duplicate {
            first_source_row_ordinal,
        })
    }

    pub fn close_and_delete(mut self) -> Result<(), StorageError> {
        self.closed = true;
        drop(self.connection.take());
        if let Some(lock_file) = self.lock_file.take() {
            let _ = FileExt::unlock(&lock_file);
        }
        remove_if_created(&self.sqlite_path)?;
        remove_if_created(&self.lock_path)?;
        Ok(())
    }
}

impl Drop for DedupIndex {
    fn drop(&mut self) {
        if self.closed {
            return;
        }
        drop(self.connection.take());
        if let Some(lock_file) = self.lock_file.take() {
            let _ = FileExt::unlock(&lock_file);
        }
        if self.created_sqlite {
            let _ = fs::remove_file(&self.sqlite_path);
        }
        if self.created_lock {
            let _ = fs::remove_file(&self.lock_path);
        }
    }
}

fn open_inner(
    lock_path: &Path,
    sqlite_path: &Path,
    run_id: Uuid,
    bundle_id: Uuid,
    started_at: DateTime<Utc>,
    created_lock: &mut bool,
    created_sqlite: &mut bool,
) -> Result<DedupIndex, StorageError> {
    let lock_file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(lock_path)
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                StorageError::AlreadyExists(run_id)
            } else {
                StorageError::io("create dedup lock", &error)
            }
        })?;
    *created_lock = true;
    apply_owner_mode(lock_path);
    if let Err(error) = FileExt::try_lock_exclusive(&lock_file) {
        if error.kind() == std::io::ErrorKind::WouldBlock {
            return Err(StorageError::AlreadyExists(run_id));
        }
        return Err(StorageError::io("lock dedup index", &error));
    }

    let sqlite_file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(sqlite_path)
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                StorageError::AlreadyExists(run_id)
            } else {
                StorageError::io("create dedup sqlite", &error)
            }
        })?;
    drop(sqlite_file);
    *created_sqlite = true;
    apply_owner_mode(sqlite_path);

    let connection =
        Connection::open(sqlite_path).map_err(|_| StorageError::database("open dedup sqlite"))?;
    connection
        .busy_timeout(std::time::Duration::from_millis(5_000))
        .map_err(|_| StorageError::database("set dedup busy timeout"))?;
    connection
        .pragma_update(None, "page_size", 4096)
        .map_err(|_| StorageError::database("set dedup page_size"))?;
    connection
        .pragma_update(None, "max_page_count", MAX_DEDUP_INDEX_PAGES)
        .map_err(|_| StorageError::database("set dedup max_page_count"))?;
    connection
        .pragma_update(None, "cache_size", -512)
        .map_err(|_| StorageError::database("set dedup cache_size"))?;
    connection
        .pragma_update(None, "journal_mode", "DELETE")
        .map_err(|_| StorageError::database("set dedup journal_mode"))?;
    connection
        .execute_batch(
            "CREATE TABLE dedup_index (
                 node_id BLOB NOT NULL,
                 rule_ordinal INTEGER NOT NULL,
                 key_bytes BLOB NOT NULL,
                 first_source_row_ordinal INTEGER NOT NULL,
                 PRIMARY KEY (node_id, rule_ordinal, key_bytes)
             ) WITHOUT ROWID;
             CREATE TABLE dedup_lease (
                 run_id BLOB NOT NULL PRIMARY KEY,
                 bundle_id BLOB NOT NULL,
                 started_at_utc TEXT NOT NULL
             ) WITHOUT ROWID;",
        )
        .map_err(|_| StorageError::database("create dedup tables"))?;
    connection
        .execute(
            "INSERT INTO dedup_lease(run_id, bundle_id, started_at_utc) VALUES (?1, ?2, ?3)",
            params![
                run_id.as_bytes().as_slice(),
                bundle_id.as_bytes().as_slice(),
                format_timestamp(&started_at)
            ],
        )
        .map_err(|_| StorageError::database("insert dedup lease"))?;

    Ok(DedupIndex {
        connection: Some(connection),
        lock_file: Some(lock_file),
        sqlite_path: sqlite_path.to_path_buf(),
        lock_path: lock_path.to_path_buf(),
        created_sqlite: *created_sqlite,
        created_lock: *created_lock,
        closed: false,
    })
}

fn create_temp_dir(path: &Path) -> Result<(), StorageError> {
    match fs::create_dir(path) {
        Ok(()) => {
            apply_owner_dir_mode(path);
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(StorageError::io("create dedup temp directory", &error)),
    }
}

fn rollback_created(
    lock_path: &Path,
    created_lock: bool,
    sqlite_path: &Path,
    created_sqlite: bool,
) {
    if created_sqlite {
        let _ = fs::remove_file(sqlite_path);
    }
    if created_lock {
        let _ = fs::remove_file(lock_path);
    }
}

fn remove_if_created(path: &Path) -> Result<(), StorageError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(StorageError::io("remove dedup file", &error)),
    }
}

#[cfg(unix)]
fn apply_owner_mode(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    if let Ok(metadata) = fs::metadata(path) {
        let mut permissions = metadata.permissions();
        permissions.set_mode(0o600);
        let _ = fs::set_permissions(path, permissions);
    }
}

#[cfg(not(unix))]
fn apply_owner_mode(_path: &Path) {}

#[cfg(unix)]
fn apply_owner_dir_mode(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    if let Ok(metadata) = fs::metadata(path) {
        let mut permissions = metadata.permissions();
        permissions.set_mode(0o700);
        let _ = fs::set_permissions(path, permissions);
    }
}

#[cfg(not(unix))]
fn apply_owner_dir_mode(_path: &Path) {}
