//! Maintenance-gated, create-new backup and restore for the managed storage root.
//!
//! A backup contains the checkpointed SQLite metadata database and immutable
//! `partitions/` files. Transient staging, lock, WAL/SHM, and external Export
//! destinations are deliberately outside this format.

use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    acquire_maintenance, ensure_managed_directory, ensure_private_directory, open_connection,
    sync_directory, SnapshotStore, StorageError, StoreInner, STORAGE_SCHEMA_VERSION,
};

pub const BACKUP_FORMAT_VERSION: u16 = 1;
pub const BACKUP_MANIFEST_FILE: &str = "backup.json";

const MAX_BACKUP_FILES: usize = 1_000_000;
const MAX_BACKUP_BYTES: u64 = 1_u64 << 42;
const MAX_BACKUP_MANIFEST_BYTES: u64 = 64 * 1024 * 1024;

/// One immutable file in a backup, addressed only by a bounded relative path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupFile {
    relative_path: String,
    byte_count: u64,
    sha256: String,
}

impl BackupFile {
    pub fn relative_path(&self) -> &str {
        &self.relative_path
    }

    pub const fn byte_count(&self) -> u64 {
        self.byte_count
    }

    pub fn sha256(&self) -> &str {
        &self.sha256
    }
}

/// Versioned manifest for one complete managed-root backup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupManifest {
    format_version: u16,
    storage_schema_version: u16,
    created_at_utc: DateTime<Utc>,
    file_count: u32,
    total_bytes: u64,
    files: Vec<BackupFile>,
}

impl BackupManifest {
    pub const fn format_version(&self) -> u16 {
        self.format_version
    }

    pub const fn storage_schema_version(&self) -> u16 {
        self.storage_schema_version
    }

    pub const fn created_at_utc(&self) -> &DateTime<Utc> {
        &self.created_at_utc
    }

    pub const fn file_count(&self) -> u32 {
        self.file_count
    }

    pub const fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    pub fn files(&self) -> &[BackupFile] {
        &self.files
    }

    fn validate(&self) -> Result<(), StorageError> {
        if self.format_version != BACKUP_FORMAT_VERSION {
            return Err(StorageError::InvalidManifest(
                "backup format version is unsupported",
            ));
        }
        if self.storage_schema_version != STORAGE_SCHEMA_VERSION {
            return Err(StorageError::InvalidManifest(
                "backup storage schema version is unsupported",
            ));
        }
        if self.files.len() > MAX_BACKUP_FILES {
            return Err(StorageError::InvalidManifest(
                "backup file count exceeds the supported bound",
            ));
        }
        let file_count = u32::try_from(self.files.len())
            .map_err(|_| StorageError::InvalidManifest("backup file count overflows"))?;
        if self.file_count != file_count {
            return Err(StorageError::InvalidManifest("backup file count mismatch"));
        }

        let mut previous: Option<&str> = None;
        let mut total_bytes = 0_u64;
        let mut metadata_seen = false;
        for file in &self.files {
            validate_relative_file_path(&file.relative_path)?;
            if let Some(previous) = previous {
                if previous >= file.relative_path.as_str() {
                    return Err(StorageError::InvalidManifest(
                        "backup files are not strictly sorted",
                    ));
                }
            }
            previous = Some(&file.relative_path);
            if file.relative_path == "metadata.sqlite3" {
                if metadata_seen {
                    return Err(StorageError::InvalidManifest(
                        "backup metadata database is duplicated",
                    ));
                }
                metadata_seen = true;
            }
            if file.byte_count == 0 {
                return Err(StorageError::InvalidManifest(
                    "backup files must be non-empty",
                ));
            }
            if file.sha256.len() != 64
                || !file
                    .sha256
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            {
                return Err(StorageError::InvalidManifest(
                    "backup file digest is not lowercase hexadecimal",
                ));
            }
            total_bytes = total_bytes
                .checked_add(file.byte_count)
                .ok_or(StorageError::ArithmeticOverflow("backup byte count"))?;
            if total_bytes > MAX_BACKUP_BYTES {
                return Err(StorageError::InvalidManifest(
                    "backup byte count exceeds the supported bound",
                ));
            }
        }
        if !metadata_seen {
            return Err(StorageError::InvalidManifest(
                "backup metadata database is missing",
            ));
        }
        if total_bytes != self.total_bytes {
            return Err(StorageError::InvalidManifest("backup byte count mismatch"));
        }
        Ok(())
    }
}

impl SnapshotStore {
    /// Creates a complete, maintenance-gated backup at a new directory.
    pub fn backup(&self, destination: impl AsRef<Path>) -> Result<BackupManifest, StorageError> {
        let _maintenance = acquire_maintenance(&self.inner)?;
        let (parent, destination) = prepare_new_destination(destination.as_ref(), "backup")?;
        let mut staging = StagingDirectory::new(&parent, destination.file_name())?;
        ensure_managed_directory(
            &staging.path().join("partitions"),
            "prepare backup partitions root",
        )?;

        checkpoint_database(&self.inner)?;
        let source_files = source_files(&self.inner.root)?;
        let mut files = Vec::with_capacity(source_files.len());
        for relative_path in source_files {
            let source = self.inner.root.join(&relative_path);
            let target = staging.path().join(&relative_path);
            let (byte_count, sha256) = copy_file(&source, &target, None)?;
            files.push(BackupFile {
                relative_path: manifest_path(&relative_path)?,
                byte_count,
                sha256,
            });
        }

        files.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
        let file_count = u32::try_from(files.len())
            .map_err(|_| StorageError::InvalidManifest("backup file count overflows"))?;
        let total_bytes = files.iter().try_fold(0_u64, |total, file| {
            total
                .checked_add(file.byte_count)
                .ok_or(StorageError::ArithmeticOverflow("backup byte count"))
        })?;
        let manifest = BackupManifest {
            format_version: BACKUP_FORMAT_VERSION,
            storage_schema_version: STORAGE_SCHEMA_VERSION,
            created_at_utc: Utc::now(),
            file_count,
            total_bytes,
            files,
        };
        manifest.validate()?;
        write_manifest(staging.path(), &manifest)?;
        sync_tree(staging.path())?;
        staging.publish(&destination)?;
        Ok(manifest)
    }

    /// Restores a validated backup into a new managed-root directory.
    pub fn restore(
        backup: impl AsRef<Path>,
        destination: impl AsRef<Path>,
    ) -> Result<BackupManifest, StorageError> {
        let backup = canonical_backup_root(backup.as_ref())?;
        let manifest = read_manifest(&backup)?;
        manifest.validate()?;
        validate_backup_shape(&backup, &manifest)?;

        let (parent, destination) = prepare_new_destination(destination.as_ref(), "restore")?;
        let mut staging = StagingDirectory::new(&parent, destination.file_name())?;
        ensure_restore_directories(staging.path())?;

        for file in &manifest.files {
            let relative_path = Path::new(&file.relative_path);
            let source = backup.join(relative_path);
            let target = staging.path().join(relative_path);
            copy_file(&source, &target, Some(file))?;
        }
        validate_database(&staging.path().join("metadata.sqlite3"))?;
        sync_tree(staging.path())?;
        staging.publish(&destination)?;
        Ok(manifest)
    }
}

struct StagingDirectory {
    path: Option<PathBuf>,
}

impl StagingDirectory {
    fn new(
        parent: &Path,
        destination_name: Option<&std::ffi::OsStr>,
    ) -> Result<Self, StorageError> {
        let name = destination_name
            .ok_or(StorageError::InvalidConfiguration(
                "backup destination name is missing",
            ))?
            .to_string_lossy();
        let path = parent.join(format!(".{name}.stillflow-backup-{}", Uuid::new_v4()));
        fs::create_dir(&path)
            .map_err(|error| StorageError::io("create backup staging root", &error))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
                .map_err(|error| StorageError::io("restrict backup staging root", &error))?;
        }
        Ok(Self { path: Some(path) })
    }

    fn path(&self) -> &Path {
        self.path.as_deref().expect("staging path is present")
    }

    fn publish(&mut self, destination: &Path) -> Result<(), StorageError> {
        if fs::symlink_metadata(destination).is_ok() {
            return Err(StorageError::InvalidConfiguration(
                "backup destination appeared during publication",
            ));
        }
        let source = self.path.take().expect("staging path is present");
        fs::rename(source, destination)
            .map_err(|error| StorageError::io("publish backup directory", &error))?;
        sync_directory(
            destination
                .parent()
                .ok_or(StorageError::InvalidConfiguration(
                    "backup destination parent is missing",
                ))?,
        )?;
        Ok(())
    }
}

impl Drop for StagingDirectory {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            let _ = fs::remove_dir_all(path);
        }
    }
}

fn prepare_new_destination(
    requested: &Path,
    operation: &'static str,
) -> Result<(PathBuf, PathBuf), StorageError> {
    let parent_requested = requested.parent().unwrap_or_else(|| Path::new("."));
    let name = requested
        .file_name()
        .ok_or(StorageError::InvalidConfiguration(
            "backup destination name is missing",
        ))?;
    if name == "." || name == ".." {
        return Err(StorageError::InvalidConfiguration(
            "backup destination name is invalid",
        ));
    }
    let parent_metadata = fs::symlink_metadata(parent_requested)
        .map_err(|error| StorageError::io("inspect backup destination parent", &error))?;
    if parent_metadata.file_type().is_symlink() || !parent_metadata.is_dir() {
        return Err(StorageError::InvalidConfiguration(
            "backup destination parent must be a non-symlink directory",
        ));
    }
    let parent = fs::canonicalize(parent_requested)
        .map_err(|error| StorageError::io("canonicalize backup destination parent", &error))?;
    let destination = parent.join(name);
    if fs::symlink_metadata(&destination).is_ok() {
        return Err(StorageError::InvalidConfiguration(match operation {
            "backup" => "backup destination already exists",
            "restore" => "restore destination already exists",
            _ => "backup destination already exists",
        }));
    }
    Ok((parent, destination))
}

fn canonical_backup_root(path: &Path) -> Result<PathBuf, StorageError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| StorageError::io("inspect backup root", &error))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(StorageError::InvalidManifest(
            "backup root must be a non-symlink directory",
        ));
    }
    fs::canonicalize(path).map_err(|error| StorageError::io("canonicalize backup root", &error))
}

fn checkpoint_database(inner: &StoreInner) -> Result<(), StorageError> {
    let connection = open_connection(inner)?;
    connection
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .map_err(|_| StorageError::database("checkpoint backup metadata database"))?;
    drop(connection);
    for suffix in ["-wal", "-shm"] {
        let path = inner.root.join(format!("metadata.sqlite3{suffix}"));
        if let Ok(metadata) = fs::symlink_metadata(path) {
            if metadata.file_type().is_symlink() || (suffix == "-wal" && metadata.len() != 0) {
                return Err(StorageError::InvalidManifest(
                    "metadata database has uncheckpointed WAL residue",
                ));
            }
        }
    }
    Ok(())
}

fn source_files(root: &Path) -> Result<Vec<PathBuf>, StorageError> {
    let database = root.join("metadata.sqlite3");
    require_regular_file(&database, "backup metadata database")?;

    let partitions = root.join("partitions");
    require_directory(&partitions, "backup partitions root")?;
    let mut files = vec![PathBuf::from("metadata.sqlite3")];
    let directories = sorted_entries(&partitions, "read backup partitions root")?;
    for (name, path) in directories {
        let identity = name.to_string_lossy();
        if Uuid::parse_str(&identity).is_err() {
            return Err(StorageError::InvalidManifest(
                "backup partitions root contains an invalid directory",
            ));
        }
        require_directory(&path, "inspect backup partition directory")?;
        for (file_name, file_path) in sorted_entries(&path, "read backup partition directory")? {
            if !is_safe_file_name(&file_name.to_string_lossy()) {
                return Err(StorageError::InvalidManifest(
                    "backup partition file name is invalid",
                ));
            }
            require_regular_file(&file_path, "backup partition file")?;
            files.push(PathBuf::from("partitions").join(&name).join(file_name));
        }
    }
    Ok(files)
}

fn copy_file(
    source: &Path,
    target: &Path,
    expected: Option<&BackupFile>,
) -> Result<(u64, String), StorageError> {
    require_regular_file(source, "read backup file")?;
    if let Some(parent) = target.parent() {
        create_directory_chain(parent)?;
    }
    let mut input =
        File::open(source).map_err(|error| StorageError::io("open backup file", &error))?;
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(target)
        .map_err(|error| StorageError::io("create backup file", &error))?;
    let mut digest = Sha256::new();
    let mut total = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = input
            .read(&mut buffer)
            .map_err(|error| StorageError::io("read backup file", &error))?;
        if read == 0 {
            break;
        }
        output
            .write_all(&buffer[..read])
            .map_err(|error| StorageError::io("write backup file", &error))?;
        digest.update(&buffer[..read]);
        total = total
            .checked_add(
                u64::try_from(read)
                    .map_err(|_| StorageError::ArithmeticOverflow("backup file byte count"))?,
            )
            .ok_or(StorageError::ArithmeticOverflow("backup file byte count"))?;
    }
    output
        .sync_all()
        .map_err(|error| StorageError::io("synchronize backup file", &error))?;
    let sha256 = hex_digest(&digest.finalize());
    if let Some(expected) = expected {
        if expected.byte_count != total || expected.sha256 != sha256 {
            return Err(StorageError::InvalidManifest(
                "backup file size or digest does not match its manifest",
            ));
        }
    }
    Ok((total, sha256))
}

fn write_manifest(root: &Path, manifest: &BackupManifest) -> Result<(), StorageError> {
    let bytes = serde_json::to_vec(manifest)
        .map_err(|_| StorageError::Serialization("encode backup manifest"))?;
    let path = root.join(BACKUP_MANIFEST_FILE);
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| StorageError::io("create backup manifest", &error))?;
    file.write_all(&bytes)
        .map_err(|error| StorageError::io("write backup manifest", &error))?;
    file.sync_all()
        .map_err(|error| StorageError::io("synchronize backup manifest", &error))
}

fn read_manifest(root: &Path) -> Result<BackupManifest, StorageError> {
    let path = root.join(BACKUP_MANIFEST_FILE);
    require_regular_file(&path, "backup manifest")?;
    let size = fs::metadata(&path)
        .map_err(|error| StorageError::io("inspect backup manifest", &error))?
        .len();
    if size > MAX_BACKUP_MANIFEST_BYTES {
        return Err(StorageError::InvalidManifest(
            "backup manifest exceeds the supported bound",
        ));
    }
    let bytes = fs::read(path).map_err(|error| StorageError::io("read backup manifest", &error))?;
    serde_json::from_slice(&bytes)
        .map_err(|_| StorageError::Serialization("decode backup manifest"))
}

fn validate_backup_shape(root: &Path, manifest: &BackupManifest) -> Result<(), StorageError> {
    let actual = tree_entries(root)?;
    let mut expected = BTreeSet::new();
    expected.insert(BACKUP_MANIFEST_FILE.to_owned());
    expected.insert("partitions".to_owned());
    for file in &manifest.files {
        expected.insert(file.relative_path.clone());
        if let Some(identity) = file.relative_path.strip_prefix("partitions/") {
            let identity = identity
                .split_once('/')
                .ok_or(StorageError::InvalidManifest(
                    "backup partition path is invalid",
                ))?
                .0;
            expected.insert(format!("partitions/{identity}"));
        }
    }
    if actual != expected {
        return Err(StorageError::InvalidManifest(
            "backup contains missing or unexpected entries",
        ));
    }
    Ok(())
}

fn ensure_restore_directories(root: &Path) -> Result<(), StorageError> {
    ensure_managed_directory(&root.join("staging"), "prepare restored staging root")?;
    ensure_managed_directory(&root.join("partitions"), "prepare restored partitions root")?;
    ensure_managed_directory(
        &root.join("export-staging"),
        "prepare restored export staging root",
    )?;
    ensure_private_directory(&root.join("temp"))
}

fn validate_database(path: &Path) -> Result<(), StorageError> {
    require_regular_file(path, "restored metadata database")?;
    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|_| StorageError::database("open restored metadata database"))?;
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(|_| StorageError::database("read restored storage schema version"))?;
    if version != i64::from(STORAGE_SCHEMA_VERSION) {
        return Err(StorageError::InvalidManifest(
            "restored metadata database schema version is unsupported",
        ));
    }
    let integrity: String = connection
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .map_err(|_| StorageError::database("validate restored metadata database"))?;
    if integrity != "ok" {
        return Err(StorageError::InvalidManifest(
            "restored metadata database failed integrity check",
        ));
    }
    let mut statement = connection
        .prepare("PRAGMA foreign_key_check")
        .map_err(|_| StorageError::database("prepare restored foreign-key check"))?;
    let mut rows = statement
        .query([])
        .map_err(|_| StorageError::database("read restored foreign-key check"))?;
    if rows
        .next()
        .map_err(|_| StorageError::database("read restored foreign-key check"))?
        .is_some()
    {
        return Err(StorageError::InvalidManifest(
            "restored metadata database failed foreign-key check",
        ));
    }
    Ok(())
}

fn tree_entries(root: &Path) -> Result<BTreeSet<String>, StorageError> {
    let mut entries = BTreeSet::new();
    collect_tree_entries(root, Path::new(""), &mut entries)?;
    Ok(entries)
}

fn collect_tree_entries(
    root: &Path,
    relative: &Path,
    entries: &mut BTreeSet<String>,
) -> Result<(), StorageError> {
    for (name, path) in sorted_entries(root, "read backup entries")? {
        let metadata = fs::symlink_metadata(&path)
            .map_err(|error| StorageError::io("inspect backup entry", &error))?;
        let child = relative.join(&name);
        let child_name = manifest_path(&child)?;
        if metadata.file_type().is_symlink() {
            return Err(StorageError::InvalidManifest("backup contains a symlink"));
        }
        if metadata.is_dir() {
            entries.insert(child_name);
            collect_tree_entries(&path, &child, entries)?;
        } else if metadata.is_file() {
            entries.insert(child_name);
        } else {
            return Err(StorageError::InvalidManifest(
                "backup contains a non-regular entry",
            ));
        }
    }
    Ok(())
}

fn sorted_entries(
    root: &Path,
    operation: &'static str,
) -> Result<Vec<(std::ffi::OsString, PathBuf)>, StorageError> {
    let mut entries = Vec::new();
    for entry in fs::read_dir(root).map_err(|error| StorageError::io(operation, &error))? {
        let entry = entry.map_err(|error| StorageError::io(operation, &error))?;
        entries.push((entry.file_name(), entry.path()));
    }
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(entries)
}

fn create_directory_chain(path: &Path) -> Result<(), StorageError> {
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                return Err(StorageError::InvalidManifest(
                    "backup parent is not a non-symlink directory",
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => fs::create_dir(&current)
                .map_err(|error| StorageError::io("create backup parent", &error))?,
            Err(error) => return Err(StorageError::io("inspect backup parent", &error)),
        }
    }
    Ok(())
}

fn sync_tree(root: &Path) -> Result<(), StorageError> {
    for (_, path) in sorted_entries(root, "read backup tree")? {
        let metadata = fs::symlink_metadata(&path)
            .map_err(|error| StorageError::io("inspect backup tree", &error))?;
        if metadata.is_dir() {
            sync_tree(&path)?;
        }
    }
    sync_directory(root)
}

fn require_regular_file(path: &Path, operation: &'static str) -> Result<(), StorageError> {
    let metadata =
        fs::symlink_metadata(path).map_err(|error| StorageError::io(operation, &error))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(StorageError::InvalidManifest(
            "backup entry must be a non-symlink regular file",
        ));
    }
    Ok(())
}

fn require_directory(path: &Path, operation: &'static str) -> Result<(), StorageError> {
    let metadata =
        fs::symlink_metadata(path).map_err(|error| StorageError::io(operation, &error))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(StorageError::InvalidManifest(
            "backup entry must be a non-symlink directory",
        ));
    }
    Ok(())
}

fn validate_relative_file_path(path: &str) -> Result<(), StorageError> {
    let parts: Vec<&str> = path.split('/').collect();
    let valid = if parts.as_slice() == ["metadata.sqlite3"] {
        true
    } else if parts.len() == 3 && parts[0] == "partitions" {
        Uuid::parse_str(parts[1]).is_ok() && is_safe_file_name(parts[2])
    } else {
        false
    };
    if !valid
        || path.contains('\\')
        || parts
            .iter()
            .any(|part| part.is_empty() || *part == "." || *part == "..")
    {
        return Err(StorageError::InvalidManifest(
            "backup file path is outside the managed format",
        ));
    }
    Ok(())
}

fn is_safe_file_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 255
        && name != "."
        && name != ".."
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
}

fn manifest_path(path: &Path) -> Result<String, StorageError> {
    let text = path
        .to_str()
        .ok_or(StorageError::InvalidManifest("backup path is not UTF-8"))?;
    if text.contains('\\') {
        return Err(StorageError::InvalidManifest(
            "backup path contains a backslash",
        ));
    }
    Ok(text.to_owned())
}

fn hex_digest(digest: &[u8]) -> String {
    let mut text = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut text, "{byte:02x}").expect("writing to String cannot fail");
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    use crate::{SnapshotStore, StorageLimits};
    #[test]
    fn backup_and_restore_round_trip_checkpointed_metadata() {
        let source_root = TempDir::new().expect("source root");
        let backup_parent = TempDir::new().expect("backup parent");
        let restore_parent = TempDir::new().expect("restore parent");
        let store =
            SnapshotStore::open(source_root.path(), StorageLimits::default()).expect("open source");
        let control = store.control_plane();
        let workspace_id = Uuid::from_u128(1);
        control
            .create_workspace(workspace_id, Utc::now())
            .expect("create workspace");
        let partition_id = Uuid::from_u128(2);
        let partition_directory = source_root
            .path()
            .join("partitions")
            .join(partition_id.to_string());
        fs::create_dir(&partition_directory).expect("partition directory");
        let partition_path = partition_directory.join("0000000000-test.parquet");
        fs::write(&partition_path, b"immutable partition bytes").expect("partition bytes");

        let backup_path = backup_parent.path().join("backup");
        let manifest = store.backup(&backup_path).expect("backup");
        assert_eq!(manifest.format_version(), BACKUP_FORMAT_VERSION);
        assert_eq!(manifest.storage_schema_version(), STORAGE_SCHEMA_VERSION);
        assert_eq!(manifest.file_count(), 2);
        assert!(backup_path.join(BACKUP_MANIFEST_FILE).is_file());
        drop(store);

        let restore_path = restore_parent.path().join("restored");
        let restored_manifest =
            SnapshotStore::restore(&backup_path, &restore_path).expect("restore");
        assert_eq!(manifest, restored_manifest);
        assert_eq!(
            fs::read(
                restore_path
                    .join("partitions")
                    .join(partition_id.to_string())
                    .join("0000000000-test.parquet")
            )
            .expect("restored partition"),
            b"immutable partition bytes"
        );
        let restored =
            SnapshotStore::open(&restore_path, StorageLimits::default()).expect("open restored");
        assert_eq!(
            restored
                .control_plane()
                .get_workspace(workspace_id)
                .expect("workspace")
                .id,
            workspace_id
        );
    }

    #[test]
    fn backup_and_restore_refuse_overwrite_and_tampering() {
        let source_root = TempDir::new().expect("source root");
        let backup_parent = TempDir::new().expect("backup parent");
        let restore_parent = TempDir::new().expect("restore parent");
        let store =
            SnapshotStore::open(source_root.path(), StorageLimits::default()).expect("open source");
        let backup_path = backup_parent.path().join("backup");
        store.backup(&backup_path).expect("backup");
        assert!(matches!(
            store.backup(&backup_path),
            Err(StorageError::InvalidConfiguration(
                "backup destination already exists"
            ))
        ));

        let manifest_path = backup_path.join(BACKUP_MANIFEST_FILE);
        let mut manifest = read_manifest(&backup_path).expect("read manifest");
        manifest.total_bytes += 1;
        let bytes = serde_json::to_vec(&manifest).expect("encode manifest");
        fs::write(&manifest_path, bytes).expect("tamper manifest");
        let restore_path = restore_parent.path().join("restored");
        assert!(matches!(
            SnapshotStore::restore(&backup_path, &restore_path),
            Err(StorageError::InvalidManifest("backup byte count mismatch"))
        ));
    }

    #[test]
    fn manifest_rejects_unbounded_or_external_paths() {
        let manifest = BackupManifest {
            format_version: BACKUP_FORMAT_VERSION,
            storage_schema_version: STORAGE_SCHEMA_VERSION,
            created_at_utc: Utc::now(),
            file_count: 1,
            total_bytes: 1,
            files: vec![BackupFile {
                relative_path: "../metadata.sqlite3".to_owned(),
                byte_count: 1,
                sha256: "0".repeat(64),
            }],
        };
        assert!(matches!(
            manifest.validate(),
            Err(StorageError::InvalidManifest(
                "backup file path is outside the managed format"
            ))
        ));
    }
}
