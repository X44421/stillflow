//! Export Manifest persistence, digests, staging, publication, recovery,
//! tombstones, and retention for the X-R1 export plane (ADR-004 §§5–8).
//!
//! The export publication machinery mirrors the snapshot discipline of
//! `store.rs`: staged writes with `create_new`, durable journal rows before
//! every destination rename, rename installation, and one atomic SQLite
//! manifest commit as the single visibility point. Digests reuse the
//! `ContentDigest` SHA-256 authority; maintenance reuses the existing
//! maintenance gate; no second publication path exists.

use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use serde::{de::Error as DeError, Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use stillflow_core::{
    validate_export_component, ExportDestination, ExportFormat, ExportInputIdentity, ExportPolicy,
    ExportShape, EXPORT_ENCODER_VERSION, EXPORT_FORMAT_CONTRACT_VERSION,
    EXPORT_JSONL_FLOAT_ENCODER, EXPORT_MANIFEST_VERSION, EXPORT_TEXT_FLOAT_ENCODER,
    MAX_EXPORT_OUTPUT_BYTES, MAX_EXPORT_PARTITIONS, MAX_EXPORT_PATH_DEPTH, MAX_EXPORT_ROWS,
    MAX_EXPORT_SINGLE_FILE_BYTES, MAX_EXPORT_TEMP_BYTES,
};

use crate::{
    create_exact_directory, digest_file, ensure_managed_directory, format_timestamp,
    open_connection, parse_timestamp, sync_directory, GarbageCollectionReport, RecoveryReport,
    StorageError, STORAGE_SCHEMA_VERSION,
};

use crate::store::{
    acquire_activity, export_staging_root, ActivityGuard, ActivityKind, StoreInner,
};

// ---------------------------------------------------------------------------
// Manifest
// ---------------------------------------------------------------------------

/// One finalized artifact file recorded in an Export Manifest (ADR-004 §7).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportManifestFile {
    name: String,
    byte_count: u64,
    digest: String,
}

impl ExportManifestFile {
    pub fn try_new(
        name: impl Into<String>,
        byte_count: u64,
        digest: impl Into<String>,
    ) -> Result<Self, StorageError> {
        let digest = digest.into();
        if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(StorageError::InvalidManifest(
                "export file digest is not SHA-256 hex",
            ));
        }
        Ok(Self {
            name: name.into(),
            byte_count,
            digest: digest.to_ascii_lowercase(),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub const fn byte_count(&self) -> u64 {
        self.byte_count
    }

    pub fn digest(&self) -> &str {
        &self.digest
    }
}

impl Serialize for ExportManifestFile {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Data<'a> {
            name: &'a str,
            byte_count: u64,
            digest: &'a str,
        }
        Data {
            name: &self.name,
            byte_count: self.byte_count,
            digest: &self.digest,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ExportManifestFile {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Data {
            name: String,
            byte_count: u64,
            digest: String,
        }
        let data = Data::deserialize(deserializer)?;
        Self::try_new(data.name, data.byte_count, data.digest).map_err(DeError::custom)
    }
}

/// Computes the Export Manifest set digest: SHA-256 over the UTF-8
/// LF-joined sequence of lowercase-hex per-file digests in partition order
/// (ADR-004 §7). The joined form has no trailing line feed; an empty file
/// list digests the empty string.
pub fn compute_export_set_digest<I>(per_file_digests: I) -> String
where
    I: IntoIterator,
    I::Item: AsRef<str>,
{
    let mut hasher = Sha256::new();
    let mut first = true;
    for digest in per_file_digests {
        if !first {
            hasher.update(b"\n");
        }
        first = false;
        hasher.update(digest.as_ref().as_bytes());
    }
    hex_lower(&hasher.finalize())
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(text, "{byte:02x}");
    }
    text
}

/// The versioned Export Manifest persisted beside the artifact set
/// (ADR-004 §7). Manifests never contain credentials, secret material, or
/// cell values; the destination root reference is the managed Allowed Root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportManifest {
    manifest_version: u16,
    export_id: Uuid,
    input: ExportInputIdentity,
    format: ExportFormat,
    shape: ExportShape,
    format_contract_version: u16,
    encoder_version: String,
    jsonl_float_encoder: String,
    text_float_encoder: String,
    storage_schema_version: u16,
    engine_contract_version: u16,
    created_at: DateTime<Utc>,
    row_count: u64,
    byte_count: u64,
    files: Vec<ExportManifestFile>,
    set_digest: String,
    destination_root: PathBuf,
    destination_relative: Vec<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExportManifestData {
    manifest_version: u16,
    export_id: Uuid,
    input: ExportInputIdentity,
    format: ExportFormat,
    shape: ExportShape,
    format_contract_version: u16,
    encoder_version: String,
    jsonl_float_encoder: String,
    text_float_encoder: String,
    storage_schema_version: u16,
    engine_contract_version: u16,
    created_at: DateTime<Utc>,
    row_count: u64,
    byte_count: u64,
    files: Vec<ExportManifestFile>,
    set_digest: String,
    destination_root: PathBuf,
    destination_relative: Vec<String>,
}

impl Serialize for ExportManifest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        ExportManifestData {
            manifest_version: self.manifest_version,
            export_id: self.export_id,
            input: self.input,
            format: self.format,
            shape: self.shape,
            format_contract_version: self.format_contract_version,
            encoder_version: self.encoder_version.clone(),
            jsonl_float_encoder: self.jsonl_float_encoder.clone(),
            text_float_encoder: self.text_float_encoder.clone(),
            storage_schema_version: self.storage_schema_version,
            engine_contract_version: self.engine_contract_version,
            created_at: self.created_at,
            row_count: self.row_count,
            byte_count: self.byte_count,
            files: self.files.clone(),
            set_digest: self.set_digest.clone(),
            destination_root: self.destination_root.clone(),
            destination_relative: self.destination_relative.clone(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ExportManifest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let data = ExportManifestData::deserialize(deserializer)?;
        Self::try_from_data(data).map_err(DeError::custom)
    }
}

impl ExportManifest {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        export_id: Uuid,
        input: ExportInputIdentity,
        format: ExportFormat,
        shape: ExportShape,
        engine_contract_version: u16,
        created_at: DateTime<Utc>,
        row_count: u64,
        files: Vec<ExportManifestFile>,
        destination_root: PathBuf,
        destination_relative: Vec<String>,
    ) -> Result<Self, StorageError> {
        let set_digest = compute_export_set_digest(files.iter().map(ExportManifestFile::digest));
        Self::try_from_data(ExportManifestData {
            manifest_version: EXPORT_MANIFEST_VERSION,
            export_id,
            input,
            format,
            shape,
            format_contract_version: EXPORT_FORMAT_CONTRACT_VERSION,
            encoder_version: EXPORT_ENCODER_VERSION.to_owned(),
            jsonl_float_encoder: EXPORT_JSONL_FLOAT_ENCODER.to_owned(),
            text_float_encoder: EXPORT_TEXT_FLOAT_ENCODER.to_owned(),
            storage_schema_version: STORAGE_SCHEMA_VERSION,
            engine_contract_version,
            created_at,
            row_count,
            byte_count: 0,
            files,
            set_digest,
            destination_root,
            destination_relative,
        })
    }

    fn try_from_data(data: ExportManifestData) -> Result<Self, StorageError> {
        if data.manifest_version != EXPORT_MANIFEST_VERSION {
            return Err(StorageError::UnsupportedStorageVersion(i64::from(
                data.manifest_version,
            )));
        }
        if data.export_id.is_nil() {
            return Err(StorageError::InvalidManifest(
                "export identity must not be nil",
            ));
        }
        if data.format_contract_version != EXPORT_FORMAT_CONTRACT_VERSION {
            return Err(StorageError::InvalidManifest(
                "export format contract version is unsupported",
            ));
        }
        if data.destination_root.as_os_str().is_empty() || !data.destination_root.is_absolute() {
            return Err(StorageError::InvalidManifest(
                "export destination root must be absolute",
            ));
        }
        if data.destination_relative.is_empty()
            || data.destination_relative.len() > MAX_EXPORT_PATH_DEPTH
        {
            return Err(StorageError::InvalidManifest(
                "export destination depth is invalid",
            ));
        }
        for component in &data.destination_relative {
            validate_export_component(component)
                .map_err(|_| StorageError::InvalidManifest("export destination path is invalid"))?;
        }
        if data.row_count > MAX_EXPORT_ROWS {
            return Err(StorageError::ExportLimitExceeded {
                resource: "export rows",
                actual: data.row_count,
                maximum: MAX_EXPORT_ROWS,
            });
        }

        let mut byte_count = 0_u64;
        for file in &data.files {
            byte_count = byte_count
                .checked_add(file.byte_count())
                .ok_or(StorageError::ArithmeticOverflow("export byte total"))?;
        }
        if byte_count > MAX_EXPORT_OUTPUT_BYTES {
            return Err(StorageError::ExportLimitExceeded {
                resource: "export output bytes",
                actual: byte_count,
                maximum: MAX_EXPORT_OUTPUT_BYTES,
            });
        }
        for file in &data.files {
            if file.byte_count() > MAX_EXPORT_SINGLE_FILE_BYTES {
                return Err(StorageError::ExportLimitExceeded {
                    resource: "export single file bytes",
                    actual: file.byte_count(),
                    maximum: MAX_EXPORT_SINGLE_FILE_BYTES,
                });
            }
        }

        match data.shape {
            ExportShape::SingleFile => {
                if data.files.len() != 1 {
                    return Err(StorageError::InvalidManifest(
                        "single-file export manifests carry exactly one file",
                    ));
                }
                let name =
                    data.destination_relative
                        .last()
                        .ok_or(StorageError::InvalidManifest(
                            "export destination depth is invalid",
                        ))?;
                if data.files[0].name() != name.as_str() {
                    return Err(StorageError::InvalidManifest(
                        "single-file export name does not match the destination",
                    ));
                }
            }
            ExportShape::PartitionedSet => {
                if data.files.len() > MAX_EXPORT_PARTITIONS as usize {
                    return Err(StorageError::ExportLimitExceeded {
                        resource: "export partitions",
                        actual: data.files.len() as u64,
                        maximum: u64::from(MAX_EXPORT_PARTITIONS),
                    });
                }
                for (index, file) in data.files.iter().enumerate() {
                    let expected = format!("part-{:010}.{}", index, data.format.extension());
                    if file.name() != expected {
                        return Err(StorageError::InvalidManifest(
                            "partitioned export file names are not contiguous part names",
                        ));
                    }
                }
            }
        }

        // The stored set digest must equal the mechanical recomputation over
        // the ordered per-file digests (ADR-004 §7).
        let set_digest =
            compute_export_set_digest(data.files.iter().map(ExportManifestFile::digest));
        if data.set_digest != set_digest {
            return Err(StorageError::InvalidManifest(
                "export set digest does not match the per-file digests",
            ));
        }

        Ok(Self {
            manifest_version: data.manifest_version,
            export_id: data.export_id,
            input: data.input,
            format: data.format,
            shape: data.shape,
            format_contract_version: data.format_contract_version,
            encoder_version: data.encoder_version,
            jsonl_float_encoder: data.jsonl_float_encoder,
            text_float_encoder: data.text_float_encoder,
            storage_schema_version: data.storage_schema_version,
            engine_contract_version: data.engine_contract_version,
            created_at: data.created_at,
            row_count: data.row_count,
            byte_count,
            files: data.files,
            set_digest,
            destination_root: data.destination_root,
            destination_relative: data.destination_relative,
        })
    }

    pub const fn manifest_version(&self) -> u16 {
        self.manifest_version
    }

    pub const fn export_id(&self) -> Uuid {
        self.export_id
    }

    pub const fn input(&self) -> &ExportInputIdentity {
        &self.input
    }

    pub const fn format(&self) -> ExportFormat {
        self.format
    }

    pub const fn shape(&self) -> ExportShape {
        self.shape
    }

    pub const fn format_contract_version(&self) -> u16 {
        self.format_contract_version
    }

    pub fn encoder_version(&self) -> &str {
        &self.encoder_version
    }

    pub fn jsonl_float_encoder(&self) -> &str {
        &self.jsonl_float_encoder
    }

    pub fn text_float_encoder(&self) -> &str {
        &self.text_float_encoder
    }

    pub const fn storage_schema_version(&self) -> u16 {
        self.storage_schema_version
    }

    pub const fn engine_contract_version(&self) -> u16 {
        self.engine_contract_version
    }

    pub const fn created_at(&self) -> &DateTime<Utc> {
        &self.created_at
    }

    pub const fn row_count(&self) -> u64 {
        self.row_count
    }

    pub const fn byte_count(&self) -> u64 {
        self.byte_count
    }

    pub fn files(&self) -> &[ExportManifestFile] {
        &self.files
    }

    pub fn set_digest(&self) -> &str {
        &self.set_digest
    }

    pub fn destination_root(&self) -> &Path {
        &self.destination_root
    }

    pub fn destination_relative(&self) -> &[String] {
        &self.destination_relative
    }
}

// ---------------------------------------------------------------------------
// Plan and provenance
// ---------------------------------------------------------------------------

/// Storage-facing plan of one export publication (ADR-004 §2, §6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportPlan {
    export_id: Uuid,
    input: ExportInputIdentity,
    destination: ExportDestination,
    format: ExportFormat,
    policy: ExportPolicy,
}

impl ExportPlan {
    pub fn try_new(
        export_id: Uuid,
        input: ExportInputIdentity,
        destination: ExportDestination,
        format: ExportFormat,
        policy: ExportPolicy,
    ) -> Result<Self, StorageError> {
        if export_id.is_nil() {
            return Err(StorageError::InvalidDraft(
                "export identity must not be nil",
            ));
        }
        if !destination.is_local() {
            return Err(StorageError::InvalidConfiguration(
                "export destinations must be managed local roots in v1",
            ));
        }
        Ok(Self {
            export_id,
            input,
            destination,
            format,
            policy,
        })
    }

    pub const fn export_id(&self) -> Uuid {
        self.export_id
    }

    pub const fn input(&self) -> &ExportInputIdentity {
        &self.input
    }

    pub const fn destination(&self) -> &ExportDestination {
        &self.destination
    }

    pub const fn format(&self) -> ExportFormat {
        self.format
    }

    pub const fn policy(&self) -> ExportPolicy {
        self.policy
    }
}

/// Engine-supplied provenance for the manifest commit (ADR-004 §7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExportProvenance {
    pub created_at: DateTime<Utc>,
    pub row_count: u64,
    pub engine_contract_version: u16,
}

// ---------------------------------------------------------------------------
// Staged files
// ---------------------------------------------------------------------------

const EXPORT_WRITE_BUFFER_BYTES: usize = 64 * 1024;

/// Buffered write handle of one staged export file.
///
/// Byte accounting feeds the per-store-root `MAX_EXPORT_TEMP_BYTES` budget
/// at buffer-flush granularity; the finalized on-disk size is the
/// authoritative bound input at install time.
pub struct StagedExportFile {
    file: Option<File>,
    path: PathBuf,
    buffer: Vec<u8>,
    sequence: u32,
    written: u64,
    accounted: u64,
    inner: Arc<StoreInner>,
    installed: bool,
}

impl StagedExportFile {
    fn new(inner: Arc<StoreInner>, path: PathBuf, sequence: u32) -> Result<Self, StorageError> {
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|error| StorageError::io("create staged export file", &error))?;
        Ok(Self {
            file: Some(file),
            path,
            buffer: Vec::with_capacity(EXPORT_WRITE_BUFFER_BYTES),
            sequence,
            written: 0,
            accounted: 0,
            inner,
            installed: false,
        })
    }

    /// Zero-based staged sequence of this file, equal to the output part
    /// sequence of partitioned artifact sets.
    pub const fn sequence(&self) -> u32 {
        self.sequence
    }

    /// Writes bytes through the bounded buffer; staging-budget violations
    /// fail typed at the flush boundary.
    pub fn write_bytes(&mut self, data: &[u8]) -> Result<(), StorageError> {
        if self.file.is_none() {
            return Err(StorageError::InvalidDraft(
                "export staging file is already finalized",
            ));
        }
        let added = u64::try_from(data.len())
            .map_err(|_| StorageError::ArithmeticOverflow("export staged written bytes"))?;
        let mut rest = data;
        while !rest.is_empty() {
            let space = EXPORT_WRITE_BUFFER_BYTES - self.buffer.len();
            let take = space.min(rest.len());
            self.buffer.extend_from_slice(&rest[..take]);
            rest = &rest[take..];
            if self.buffer.len() == EXPORT_WRITE_BUFFER_BYTES {
                self.flush_buffer()?;
            }
        }
        self.written = self
            .written
            .checked_add(added)
            .ok_or(StorageError::ArithmeticOverflow(
                "export staged written bytes",
            ))?;
        Ok(())
    }

    /// Flushes the bounded buffer to the staged file and accounts the
    /// flushed bytes against the live staging budget.
    pub fn flush_buffer(&mut self) -> Result<(), StorageError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let file = self
            .file
            .as_mut()
            .ok_or(StorageError::InvalidDraft("export staging file is closed"))?;
        file.write_all(&self.buffer)
            .map_err(|error| StorageError::io("write staged export file", &error))?;
        let flushed = self.buffer.len();
        self.buffer.clear();
        self.account(u64::try_from(flushed).expect("buffer size fits u64"))
    }

    fn account(&mut self, bytes: u64) -> Result<(), StorageError> {
        if bytes == 0 {
            return Ok(());
        }
        account_export_staging(&self.inner, bytes)?;
        self.accounted = self
            .accounted
            .checked_add(bytes)
            .ok_or(StorageError::ArithmeticOverflow("export staging bytes"))?;
        Ok(())
    }

    /// Flushes, fsyncs the staged bytes, and re-accounts against the
    /// authoritative on-disk size (covers encoder-native writes).
    pub fn finalize(&mut self) -> Result<(), StorageError> {
        self.flush_buffer()?;
        let file = self
            .file
            .as_mut()
            .ok_or(StorageError::InvalidDraft("export staging file is closed"))?;
        file.sync_all()
            .map_err(|error| StorageError::io("sync staged export file", &error))?;
        self.refresh_accounting()?;
        Ok(())
    }

    /// Re-accounts the live staging budget against the on-disk file size.
    /// Structured encoders (Parquet) must call this after finalizing.
    pub fn refresh_accounting(&mut self) -> Result<(), StorageError> {
        let file = self
            .file
            .as_ref()
            .ok_or(StorageError::InvalidDraft("export staging file is closed"))?;
        let on_disk = file
            .metadata()
            .map_err(|error| StorageError::io("inspect staged export file", &error))?
            .len();
        if on_disk >= self.accounted {
            self.account(on_disk - self.accounted)?;
        }
        self.written = on_disk;
        Ok(())
    }

    /// Direct file access for structured encoders (Parquet). The encoder
    /// must call [`StagedExportFile::refresh_accounting`] after finalizing.
    pub fn file(&mut self) -> Result<&mut File, StorageError> {
        self.file
            .as_mut()
            .ok_or(StorageError::InvalidDraft("export staging file is closed"))
    }

    pub fn written_bytes(&self) -> u64 {
        self.written
    }

    pub fn staged_path(&self) -> &Path {
        &self.path
    }

    fn take_file(&mut self) -> Result<File, StorageError> {
        self.flush_buffer()?;
        self.file
            .take()
            .ok_or(StorageError::InvalidDraft("export staging file is closed"))
    }

    fn release_accounting(&mut self) {
        if self.accounted != 0 {
            self.inner
                .export_staging_bytes
                .fetch_sub(self.accounted, Ordering::SeqCst);
            self.accounted = 0;
        }
    }
}

impl Drop for StagedExportFile {
    fn drop(&mut self) {
        self.release_accounting();
        drop(self.file.take());
        if !self.installed {
            let _ = fs::remove_file(&self.path);
        }
    }
}

impl Write for StagedExportFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = buf.len();
        self.write_bytes(buf)
            .map(|()| written)
            .map_err(std::io::Error::other)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.flush_buffer().map_err(std::io::Error::other)
    }
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

/// Publication writer of one export. Owned by the encoding runtime from
/// staging creation through manifest commit (ADR-004 §7).
pub struct ExportWriter {
    inner: Arc<StoreInner>,
    _activity: ActivityGuard,
    plan: ExportPlan,
    started_at: DateTime<Utc>,
    staging_dir: PathBuf,
    destination_root: PathBuf,
    destination_relative: Vec<String>,
    files: Vec<ExportManifestFile>,
    total_installed_bytes: u64,
    next_staged_sequence: u32,
    installed: bool,
    committed: bool,
    failed: bool,
}

impl ExportWriter {
    /// Creates a staged file keyed by the next output sequence.
    pub fn create_staged_file(&mut self) -> Result<StagedExportFile, StorageError> {
        if self.failed {
            return Err(StorageError::InvalidDraft(
                "export writer is already in a failed state",
            ));
        }
        let sequence = self.next_staged_sequence;
        let path = self.staging_dir.join(format!("{sequence:010}.staged"));
        match fs::symlink_metadata(&path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(StorageError::io("inspect staged export file", &error)),
            Ok(_) => return Err(StorageError::ExportStagingExists(self.plan.export_id())),
        }
        let staged = StagedExportFile::new(Arc::clone(&self.inner), path, sequence)?;
        self.next_staged_sequence = self
            .next_staged_sequence
            .checked_add(1)
            .ok_or(StorageError::ArithmeticOverflow("export staged sequence"))?;
        Ok(staged)
    }

    /// Finalizes a staged file, durably journals the destination path, and
    /// installs the file by rename (ADR-004 §7 publication sequence).
    pub fn install_staged_file(
        &mut self,
        mut staged: StagedExportFile,
    ) -> Result<ExportManifestFile, StorageError> {
        if self.failed {
            return Err(StorageError::InvalidDraft(
                "export writer is already in a failed state",
            ));
        }
        let result = self.install_inner(&mut staged);
        if result.is_err() {
            self.failed = true;
        }
        // The handle is consumed either way: on success the bytes live at
        // the destination; on failure the staged bytes are removed and the
        // export aborts (definitive cleanup is the recovery sweep).
        staged.installed = result.is_ok();
        drop(staged);
        result
    }

    fn install_inner(
        &mut self,
        staged: &mut StagedExportFile,
    ) -> Result<ExportManifestFile, StorageError> {
        let sequence = staged.sequence();
        let file_name = match self.plan.policy().shape {
            ExportShape::SingleFile => self
                .destination_relative
                .last()
                .ok_or(StorageError::InvalidManifest(
                    "export destination depth is invalid",
                ))?
                .clone(),
            ExportShape::PartitionedSet => {
                format!("part-{:010}.{}", sequence, self.plan.format().extension())
            }
        };

        staged.finalize()?;
        let mut file = staged.take_file()?;
        let byte_count = file
            .metadata()
            .map_err(|error| StorageError::io("inspect finalized export file", &error))?
            .len();
        if byte_count > MAX_EXPORT_SINGLE_FILE_BYTES {
            return Err(StorageError::ExportLimitExceeded {
                resource: "export single file bytes",
                actual: byte_count,
                maximum: MAX_EXPORT_SINGLE_FILE_BYTES,
            });
        }
        let total = self
            .total_installed_bytes
            .checked_add(byte_count)
            .ok_or(StorageError::ArithmeticOverflow("export output byte total"))?;
        if total > MAX_EXPORT_OUTPUT_BYTES {
            return Err(StorageError::ExportLimitExceeded {
                resource: "export output bytes",
                actual: total,
                maximum: MAX_EXPORT_OUTPUT_BYTES,
            });
        }
        file.seek(SeekFrom::Start(0))
            .map_err(|error| StorageError::io("rewind finalized export file", &error))?;
        let digest = digest_file(&mut file)?;
        drop(file);

        let mut destination_components = self.destination_relative.clone();
        if self.plan.policy().shape == ExportShape::PartitionedSet {
            destination_components.push(file_name.clone());
        }

        // Durable journal before the destination rename (ADR-004 §7).
        journal_export_install(
            &self.inner,
            self.plan.export_id(),
            &self.destination_root,
            &destination_components,
        )?;

        // Materialize the artifact directory of a partitioned set exactly
        // once, then install by rename under a create-new precheck.
        if self.plan.policy().shape == ExportShape::PartitionedSet && !self.installed {
            let set_dir = destination_path(&self.destination_root, &self.destination_relative)?;
            if fs::symlink_metadata(&set_dir).is_ok() {
                return Err(StorageError::ExportDestinationExists(self.plan.export_id()));
            }
            create_exact_directory(&set_dir, "create export artifact directory")?;
        }
        let parent = destination_parent_path(&self.destination_root, &destination_components)?;
        let destination = destination_path(&self.destination_root, &destination_components)?;
        if fs::symlink_metadata(&destination).is_ok() {
            return Err(StorageError::ExportDestinationExists(self.plan.export_id()));
        }
        fs::rename(&staged.path, &destination)
            .map_err(|error| StorageError::io("install export artifact file", &error))?;
        sync_directory(&parent)?;
        sync_directory(&self.destination_root)?;

        let record = ExportManifestFile::try_new(file_name, byte_count, digest.to_string())?;
        self.files.push(record.clone());
        self.total_installed_bytes = total;
        self.installed = true;
        // The file left staging: release its budget contribution.
        staged.release_accounting();
        Ok(record)
    }

    /// Commits the Export Manifest: the single visibility point of the
    /// artifact set (ADR-004 §7).
    pub fn commit(mut self, provenance: ExportProvenance) -> Result<ExportManifest, StorageError> {
        if self.failed {
            return Err(StorageError::InvalidDraft(
                "export writer cannot commit after a failed install",
            ));
        }
        if provenance.created_at > self.started_at {
            return Err(StorageError::InvalidTimestampOrder(
                "export creation and publication start",
            ));
        }
        let manifest = ExportManifest::try_new(
            self.plan.export_id(),
            *self.plan.input(),
            self.plan.format(),
            self.plan.policy().shape,
            provenance.engine_contract_version,
            provenance.created_at,
            provenance.row_count,
            self.files.clone(),
            self.destination_root.clone(),
            self.destination_relative.clone(),
        )?;
        let manifest_json = serde_json::to_string(&manifest)
            .map_err(|_| StorageError::Serialization("encode export manifest"))?;

        let mut connection = open_connection(&self.inner)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StorageError::database("begin export manifest transaction"))?;
        let journal_exists: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM export_publications WHERE export_id = ?1)",
                params![self.plan.export_id().to_string()],
                |row| row.get(0),
            )
            .map_err(|_| StorageError::database("verify export publication journal"))?;
        if !journal_exists {
            return Err(StorageError::InvalidManifest(
                "export publication journal is missing",
            ));
        }
        transaction
            .execute(
                "INSERT INTO export_manifests(export_id, version, manifest_json, committed_at_utc)
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    manifest.export_id().to_string(),
                    i64::from(manifest.manifest_version()),
                    manifest_json,
                    format_timestamp(&Utc::now()),
                ],
            )
            .map_err(|_| StorageError::database("insert committed export manifest"))?;
        transaction
            .execute(
                "DELETE FROM export_publications WHERE export_id = ?1",
                params![manifest.export_id().to_string()],
            )
            .map_err(|_| StorageError::database("complete export publication journal"))?;
        transaction
            .execute(
                "DELETE FROM export_journal WHERE export_id = ?1",
                params![manifest.export_id().to_string()],
            )
            .map_err(|_| StorageError::database("complete export file journal"))?;
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit export manifest"))?;

        self.committed = true;
        // Remove the staging directory; the manifest commit is the single
        // visibility point, so this residue removal cannot hide artifacts.
        let _ = fs::remove_dir_all(&self.staging_dir);
        Ok(manifest)
    }

    pub const fn export_id(&self) -> Uuid {
        self.plan.export_id()
    }

    pub fn files(&self) -> &[ExportManifestFile] {
        &self.files
    }
}

impl Drop for ExportWriter {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        // Best-effort immediate cleanup: staging and the publication claim.
        // Journaled destination residue and its journal rows are left for
        // the definitive recovery sweep (ADR-004 §7: journal rows are
        // removed only by manifest commit or recovery).
        let _ = fs::remove_dir_all(&self.staging_dir);
        if let Ok(connection) = open_connection(&self.inner) {
            let _ = connection.execute(
                "DELETE FROM export_publications WHERE export_id = ?1",
                params![self.plan.export_id().to_string()],
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Paths and helpers
// ---------------------------------------------------------------------------

fn export_staging_dir(inner: &StoreInner, export_id: Uuid) -> PathBuf {
    export_staging_root(inner).join(export_id.to_string())
}

/// Builds a destination path below one Allowed Root from validated
/// components. Components are re-validated so no corrupt journal row can
/// traverse out of the root.
fn destination_path(root: &Path, components: &[String]) -> Result<PathBuf, StorageError> {
    validate_components(components)?;
    let mut path = root.to_path_buf();
    for component in components {
        path.push(component);
    }
    Ok(path)
}

fn destination_parent_path(root: &Path, components: &[String]) -> Result<PathBuf, StorageError> {
    if components.len() < 2 {
        return Ok(root.to_path_buf());
    }
    destination_path(root, &components[..components.len() - 1])
}

fn validate_components(components: &[String]) -> Result<(), StorageError> {
    if components.is_empty() || components.len() > MAX_EXPORT_PATH_DEPTH {
        return Err(StorageError::InvalidManifest(
            "export destination depth is invalid",
        ));
    }
    for component in components {
        validate_export_component(component)
            .map_err(|_| StorageError::InvalidManifest("export destination path is invalid"))?;
    }
    Ok(())
}

/// Validates the Allowed Root: an existing absolute directory whose
/// canonical form is byte-identical (rejects symlinks, traversal, and
/// relative roots before any byte is written, ADR-004 §6).
fn validate_destination_root(root: &Path) -> Result<(), StorageError> {
    if !root.is_absolute() {
        return Err(StorageError::InvalidConfiguration(
            "export destination root must be absolute",
        ));
    }
    let metadata = fs::symlink_metadata(root)
        .map_err(|error| StorageError::io("inspect export destination root", &error))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(StorageError::InvalidConfiguration(
            "export destination root must be a non-symlink directory",
        ));
    }
    let canonical = fs::canonicalize(root)
        .map_err(|error| StorageError::io("canonicalize export destination root", &error))?;
    if canonical != root {
        return Err(StorageError::InvalidConfiguration(
            "export destination root must be its canonical non-symlink form",
        ));
    }
    Ok(())
}

/// Creates the missing parent directories of a destination path with
/// managed-directory discipline (no symlinks, no replacement).
fn ensure_destination_parents(root: &Path, components: &[String]) -> Result<(), StorageError> {
    let mut current = root.to_path_buf();
    for component in &components[..components.len().saturating_sub(1)] {
        current.push(component);
        ensure_managed_directory(&current, "create export destination parent")?;
    }
    Ok(())
}

/// Adds `bytes` to the per-store-root live staging budget and fails typed
/// when the frozen `MAX_EXPORT_TEMP_BYTES` ceiling would be exceeded.
fn account_export_staging(inner: &Arc<StoreInner>, bytes: u64) -> Result<(), StorageError> {
    let previous = inner
        .export_staging_bytes
        .fetch_add(bytes, Ordering::SeqCst);
    let total = previous
        .checked_add(bytes)
        .ok_or(StorageError::ArithmeticOverflow("export staging bytes"))?;
    if total > MAX_EXPORT_TEMP_BYTES {
        return Err(StorageError::ExportLimitExceeded {
            resource: "export staging bytes",
            actual: total,
            maximum: MAX_EXPORT_TEMP_BYTES,
        });
    }
    Ok(())
}

fn journal_export_install(
    inner: &StoreInner,
    export_id: Uuid,
    destination_root: &Path,
    destination_relative: &[String],
) -> Result<(), StorageError> {
    let mut connection = open_connection(inner)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|_| StorageError::database("begin export journal transaction"))?;
    transaction
        .execute(
            "INSERT INTO export_journal(export_id, destination_root, destination_relative, journaled_at_utc)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                export_id.to_string(),
                destination_root.to_string_lossy(),
                serde_json::to_string(destination_relative)
                    .map_err(|_| StorageError::Serialization("encode export journal path"))?,
                format_timestamp(&Utc::now()),
            ],
        )
        .map_err(|_| StorageError::database("insert export journal row"))?;
    transaction
        .commit()
        .map_err(|_| StorageError::database("commit export journal row"))
}

// ---------------------------------------------------------------------------
// Store surface
// ---------------------------------------------------------------------------

impl crate::SnapshotStore {
    /// Begins an export publication: reserves the caller-injected id,
    /// registers the destination reference, validates the Allowed Root, and
    /// creates the staging directory (ADR-004 §6, §7).
    pub fn begin_export(
        &self,
        plan: ExportPlan,
        started_at: DateTime<Utc>,
    ) -> Result<ExportWriter, StorageError> {
        let export_id = plan.export_id();
        let (root, relative) = match plan.destination() {
            ExportDestination::Local { root, components } => (root.clone(), components.clone()),
            ExportDestination::ObjectStore { .. } => {
                return Err(StorageError::InvalidConfiguration(
                    "object-store export destinations are not available in v1",
                ));
            }
        };
        let activity = acquire_activity(&self.inner, ActivityKind::ExportPublisher)?;

        // Identity reservation: the id must be free of any export claim.
        {
            let connection = open_connection(&self.inner)?;
            let exists: bool = connection
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM export_publications WHERE export_id = ?1)
                        OR EXISTS(SELECT 1 FROM export_manifests WHERE export_id = ?1)
                        OR EXISTS(SELECT 1 FROM export_tombstones WHERE export_id = ?1)",
                    params![export_id.to_string()],
                    |row| row.get(0),
                )
                .map_err(|_| StorageError::database("check existing export identity"))?;
            if exists {
                return Err(StorageError::AlreadyExists(export_id));
            }
        }

        if let Err(error) = insert_export_publication(&self.inner, &plan, &started_at) {
            drop(activity);
            return Err(error);
        }

        if let Err(error) = (|| -> Result<(), StorageError> {
            validate_destination_root(&root)?;
            ensure_destination_parents(&root, &relative)?;
            let destination = destination_path(&root, &relative)?;
            if fs::symlink_metadata(&destination).is_ok() {
                return Err(StorageError::ExportDestinationExists(export_id));
            }
            let staging_dir = export_staging_dir(&self.inner, export_id);
            match fs::symlink_metadata(&staging_dir) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(StorageError::io("inspect export staging", &error)),
                Ok(_) => return Err(StorageError::ExportStagingExists(export_id)),
            }
            create_exact_directory(&staging_dir, "create export staging directory")?;
            Ok(())
        })() {
            // The publication claim is released; staging residue from a
            // partial creation is left for the definitive recovery sweep.
            drop(activity);
            let _ = delete_export_publication(&self.inner, export_id);
            return Err(error);
        }

        Ok(ExportWriter {
            inner: Arc::clone(&self.inner),
            _activity: activity,
            plan,
            started_at,
            staging_dir: export_staging_dir(&self.inner, export_id),
            destination_root: root,
            destination_relative: relative,
            files: Vec::new(),
            total_installed_bytes: 0,
            next_staged_sequence: 0,
            installed: false,
            committed: false,
            failed: false,
        })
    }
}

fn insert_export_publication(
    inner: &Arc<StoreInner>,
    plan: &ExportPlan,
    started_at: &DateTime<Utc>,
) -> Result<(), StorageError> {
    let (root, relative) = match plan.destination() {
        ExportDestination::Local { root, components } => (root, components),
        ExportDestination::ObjectStore { .. } => {
            return Err(StorageError::InvalidConfiguration(
                "object-store export destinations are not available in v1",
            ));
        }
    };
    let mut connection = open_connection(inner)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|_| StorageError::database("begin export publication transaction"))?;
    transaction
        .execute(
            "INSERT INTO export_publications(
                 export_id, snapshot_id, destination_root, destination_relative, started_at_utc
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                plan.export_id().to_string(),
                plan.input().snapshot_id().to_string(),
                root.to_string_lossy(),
                serde_json::to_string(relative)
                    .map_err(|_| StorageError::Serialization("encode export destination"))?,
                format_timestamp(started_at),
            ],
        )
        .map_err(|_| StorageError::database("insert export publication journal"))?;
    transaction
        .commit()
        .map_err(|_| StorageError::database("commit export publication journal"))
}

// ---------------------------------------------------------------------------
// Readers, tombstones, recovery, garbage collection
// ---------------------------------------------------------------------------

pub(crate) fn load_export_manifest_inner(
    inner: &Arc<StoreInner>,
    export_id: Uuid,
) -> Result<ExportManifest, StorageError> {
    let connection = open_connection(inner)?;
    let (version, manifest_json): (i64, String) = connection
        .query_row(
            "SELECT version, manifest_json FROM export_manifests WHERE export_id = ?1",
            params![export_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(|_| StorageError::database("load export manifest"))?
        .ok_or(StorageError::NotFound(export_id))?;
    if version != i64::from(EXPORT_MANIFEST_VERSION) {
        return Err(StorageError::UnsupportedStorageVersion(version));
    }
    let manifest: ExportManifest = serde_json::from_str(&manifest_json)
        .map_err(|_| StorageError::Serialization("decode export manifest"))?;
    if manifest.export_id() != export_id {
        return Err(StorageError::InvalidManifest(
            "export manifest identity does not match its row",
        ));
    }
    Ok(manifest)
}

pub(crate) fn tombstone_export_inner(
    inner: &Arc<StoreInner>,
    export_id: Uuid,
    tombstoned_at: &DateTime<Utc>,
) -> Result<(), StorageError> {
    let mut connection = open_connection(inner)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|_| StorageError::database("begin export tombstone transaction"))?;
    let row: Option<(i64, String, String)> = transaction
        .query_row(
            "SELECT version, manifest_json, committed_at_utc FROM export_manifests
             WHERE export_id = ?1",
            params![export_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(|_| StorageError::database("read export manifest for tombstone"))?;
    let Some((version, manifest_json, committed_at)) = row else {
        return Err(StorageError::NotFound(export_id));
    };
    if version != i64::from(EXPORT_MANIFEST_VERSION) {
        return Err(StorageError::UnsupportedStorageVersion(version));
    }
    let manifest: ExportManifest = serde_json::from_str(&manifest_json)
        .map_err(|_| StorageError::Serialization("decode export manifest"))?;
    let committed_at = parse_timestamp(&committed_at, "export commit timestamp")?;
    if tombstoned_at < &committed_at {
        return Err(StorageError::InvalidTimestampOrder(
            "export commit and tombstone",
        ));
    }
    transaction
        .execute(
            "INSERT INTO export_tombstones(export_id, destination_root, destination_relative, tombstoned_at_utc)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                export_id.to_string(),
                manifest.destination_root().to_string_lossy(),
                serde_json::to_string(manifest.destination_relative())
                    .map_err(|_| StorageError::Serialization("encode export destination"))?,
                format_timestamp(tombstoned_at),
            ],
        )
        .map_err(|_| StorageError::database("insert export tombstone"))?;
    transaction
        .execute(
            "DELETE FROM export_manifests WHERE export_id = ?1",
            params![export_id.to_string()],
        )
        .map_err(|_| StorageError::database("hide export manifest"))?;
    transaction
        .commit()
        .map_err(|_| StorageError::database("commit export tombstone"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExportResidue {
    export_id: Uuid,
    destination_root: String,
    destination_relative: String,
}

fn export_residue_from_row(
    export_id: String,
    destination_root: String,
    destination_relative: String,
) -> Result<ExportResidue, StorageError> {
    let export_id = Uuid::parse_str(&export_id)
        .map_err(|_| StorageError::InvalidManifest("export identity is invalid"))?;
    Ok(ExportResidue {
        export_id,
        destination_root,
        destination_relative,
    })
}

/// Deletes one destination path recorded by a journal or tombstone row.
/// Symlinked entries are never followed or removed (management discipline).
fn remove_destination_residue(residue: &ExportResidue) -> Result<bool, StorageError> {
    let root = PathBuf::from(&residue.destination_root);
    let components: Vec<String> = serde_json::from_str(&residue.destination_relative)
        .map_err(|_| StorageError::InvalidManifest("export journal path is invalid"))?;
    let path = destination_path(&root, &components)?;
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(true),
        Err(error) => return Err(StorageError::io("inspect export residue", &error)),
    };
    if metadata.file_type().is_symlink() {
        return Ok(false);
    }
    if metadata.is_dir() {
        fs::remove_dir_all(&path)
            .map_err(|error| StorageError::io("remove export residue directory", &error))?;
    } else {
        fs::remove_file(&path)
            .map_err(|error| StorageError::io("remove export residue file", &error))?;
    }
    Ok(true)
}

fn delete_export_journal_rows(
    inner: &Arc<StoreInner>,
    export_id: Uuid,
) -> Result<(), StorageError> {
    let connection = open_connection(inner)?;
    connection
        .execute(
            "DELETE FROM export_journal WHERE export_id = ?1",
            params![export_id.to_string()],
        )
        .map(|_| ())
        .map_err(|_| StorageError::database("delete export journal rows"))
}

fn export_journal_rows(
    inner: &Arc<StoreInner>,
    export_id: Uuid,
) -> Result<Vec<ExportResidue>, StorageError> {
    let connection = open_connection(inner)?;
    let mut statement = connection
        .prepare(
            "SELECT destination_root, destination_relative FROM export_journal
             WHERE export_id = ?1 ORDER BY journaled_at_utc, rowid",
        )
        .map_err(|_| StorageError::database("prepare export journal query"))?;
    let rows = statement
        .query_map(params![export_id.to_string()], |row| {
            Ok(ExportResidue {
                export_id,
                destination_root: row.get(0)?,
                destination_relative: row.get(1)?,
            })
        })
        .map_err(|_| StorageError::database("query export journal rows"))?;
    let mut residues = Vec::new();
    for row in rows {
        residues.push(row.map_err(|_| StorageError::database("read export journal row"))?);
    }
    Ok(residues)
}

fn export_is_committed(inner: &Arc<StoreInner>, export_id: Uuid) -> Result<bool, StorageError> {
    let connection = open_connection(inner)?;
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM export_manifests WHERE export_id = ?1)",
            params![export_id.to_string()],
            |row| row.get(0),
        )
        .map_err(|_| StorageError::database("check export manifest visibility"))
}

fn export_identity_is_claimed(
    inner: &Arc<StoreInner>,
    export_id: Uuid,
) -> Result<bool, StorageError> {
    let connection = open_connection(inner)?;
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM export_publications WHERE export_id = ?1)
                OR EXISTS(SELECT 1 FROM export_manifests WHERE export_id = ?1)
                OR EXISTS(SELECT 1 FROM export_tombstones WHERE export_id = ?1)",
            params![export_id.to_string()],
            |row| row.get(0),
        )
        .map_err(|_| StorageError::database("check export identity claims"))
}

fn remove_staging_directory(
    inner: &Arc<StoreInner>,
    export_id: Uuid,
    ignored: &mut u32,
) -> Result<(), StorageError> {
    let path = export_staging_dir(inner, export_id);
    match fs::symlink_metadata(&path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                *ignored += 1;
                return Ok(());
            }
            fs::remove_dir_all(&path)
                .map_err(|error| StorageError::io("remove export staging directory", &error))?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(StorageError::io("inspect export staging directory", &error)),
    }
}

fn delete_export_publication(inner: &Arc<StoreInner>, export_id: Uuid) -> Result<(), StorageError> {
    let connection = open_connection(inner)?;
    connection
        .execute(
            "DELETE FROM export_publications WHERE export_id = ?1",
            params![export_id.to_string()],
        )
        .map(|_| ())
        .map_err(|_| StorageError::database("delete export publication journal"))
}

/// Recovery sweep of export residue. Runs under the store maintenance gate;
/// it deletes stale staging, uncommitted journal rows, and every journaled
/// destination path. Recovery never publishes and never deletes a
/// manifest-committed artifact (ADR-004 §7).
pub(crate) fn recover_export_residue(
    inner: &Arc<StoreInner>,
    cutoff: &str,
    maximum: u32,
    report: &mut RecoveryReport,
) -> Result<(), StorageError> {
    let mut examined = 0_u32;
    let mut recovered = 0_u32;
    let mut ignored = 0_u32;

    // 1. Stale publication claims: full residue cleanup.
    let stale: Vec<ExportResidue> = {
        let connection = open_connection(inner)?;
        let mut statement = connection
            .prepare(
                "SELECT export_id, destination_root, destination_relative FROM export_publications
                 WHERE started_at_utc <= ?1 ORDER BY started_at_utc, export_id LIMIT ?2",
            )
            .map_err(|_| StorageError::database("prepare stale export publication query"))?;
        let rows = statement
            .query_map(params![cutoff, i64::from(maximum)], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(|_| StorageError::database("query stale export publications"))?;
        let mut stale = Vec::new();
        for row in rows {
            let (id, root, relative) =
                row.map_err(|_| StorageError::database("read stale export publication"))?;
            stale.push(export_residue_from_row(id, root, relative)?);
        }
        drop(statement);
        drop(connection);
        stale
    };

    for residue in stale {
        examined += 1;
        if export_is_committed(inner, residue.export_id)? {
            // A committed manifest with a lingering publication row cannot
            // arise under the atomic commit; only staging is removed.
            remove_staging_directory(inner, residue.export_id, &mut ignored)?;
        } else {
            remove_staging_directory(inner, residue.export_id, &mut ignored)?;
            for journaled in export_journal_rows(inner, residue.export_id)? {
                if !remove_destination_residue(&journaled)? {
                    ignored += 1;
                }
            }
            delete_export_journal_rows(inner, residue.export_id)?;
        }
        delete_export_publication(inner, residue.export_id)?;
        recovered += 1;
    }

    // 2. Journal rows of exports that no longer hold a publication claim
    // (gracefully aborted writers): destination residue only.
    let orphan_ids: Vec<Uuid> = {
        let connection = open_connection(inner)?;
        let mut statement = connection
            .prepare("SELECT DISTINCT export_id FROM export_journal LIMIT ?1")
            .map_err(|_| StorageError::database("prepare orphan export journal query"))?;
        let rows = statement
            .query_map(params![i64::from(maximum)], |row| row.get::<_, String>(0))
            .map_err(|_| StorageError::database("query orphan export journal ids"))?;
        let mut ids = Vec::new();
        for row in rows {
            let value = row.map_err(|_| StorageError::database("read orphan export journal id"))?;
            ids.push(
                Uuid::parse_str(&value)
                    .map_err(|_| StorageError::InvalidManifest("export identity is invalid"))?,
            );
        }
        drop(statement);
        drop(connection);
        ids
    };

    for export_id in orphan_ids {
        if export_identity_is_claimed(inner, export_id)? {
            continue;
        }
        examined += 1;
        for journaled in export_journal_rows(inner, export_id)? {
            if !remove_destination_residue(&journaled)? {
                ignored += 1;
            }
        }
        delete_export_journal_rows(inner, export_id)?;
        recovered += 1;
    }

    // 3. Orphan staging directories keyed by unclaimed ids.
    {
        let staging_root = export_staging_root(inner);
        let entries = fs::read_dir(&staging_root)
            .map_err(|error| StorageError::io("scan export staging root", &error))?;
        for (scanned, entry) in entries.enumerate() {
            if scanned >= maximum as usize {
                break;
            }
            let entry =
                entry.map_err(|error| StorageError::io("read export staging entry", &error))?;
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                ignored += 1;
                continue;
            };
            let Ok(export_id) = Uuid::parse_str(&name) else {
                ignored += 1;
                continue;
            };
            if export_identity_is_claimed(inner, export_id)? {
                continue;
            }
            examined += 1;
            remove_staging_directory(inner, export_id, &mut ignored)?;
            recovered += 1;
        }
    }

    report.add(examined, recovered, ignored);
    Ok(())
}

/// Garbage collection of tombstoned export artifacts past the explicit
/// retention cutoff, with candidate caps (ADR-004 §7).
pub(crate) fn collect_export_garbage(
    inner: &Arc<StoreInner>,
    cutoff: &str,
    maximum: u32,
    report: &mut GarbageCollectionReport,
) -> Result<(), StorageError> {
    let candidates: Vec<ExportResidue> = {
        let connection = open_connection(inner)?;
        let mut statement = connection
            .prepare(
                "SELECT export_id, destination_root, destination_relative FROM export_tombstones
                 WHERE tombstoned_at_utc <= ?1 ORDER BY tombstoned_at_utc, export_id LIMIT ?2",
            )
            .map_err(|_| StorageError::database("prepare export tombstone query"))?;
        let rows = statement
            .query_map(params![cutoff, i64::from(maximum)], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(|_| StorageError::database("query eligible export tombstones"))?;
        let mut candidates = Vec::new();
        for row in rows {
            let (id, root, relative) =
                row.map_err(|_| StorageError::database("read eligible export tombstone"))?;
            candidates.push(export_residue_from_row(id, root, relative)?);
        }
        drop(statement);
        drop(connection);
        candidates
    };

    let mut examined = 0_u32;
    let mut deleted = 0_u32;
    let mut retained = 0_u32;
    for residue in candidates {
        examined += 1;
        if remove_destination_residue(&residue)? {
            let connection = open_connection(inner)?;
            let rows = connection
                .execute(
                    "DELETE FROM export_tombstones WHERE export_id = ?1 AND tombstoned_at_utc <= ?2",
                    params![residue.export_id.to_string(), cutoff],
                )
                .map_err(|_| StorageError::database("delete collected export tombstone"))?;
            drop(connection);
            if rows == 1 {
                deleted += 1;
            } else {
                retained += 1;
            }
        } else {
            retained += 1;
        }
    }
    report.add(examined, deleted, retained);
    Ok(())
}
