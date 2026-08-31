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

    /// Decodes one stored manifest payload with every revalidation law
    /// applied (version, identity, bounds, naming, digests). Revalidation
    /// failures fail typed — never as an opaque serialization error.
    pub(crate) fn decode(json: &str) -> Result<Self, StorageError> {
        let data: ExportManifestData = serde_json::from_str(json)
            .map_err(|_| StorageError::Serialization("decode export manifest"))?;
        Self::try_from_data(data)
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
            // The set directory is itself a destination path of this export:
            // journal it before creation so the recovery sweep can free the
            // destination name (ADR-004 §7).
            journal_export_install(
                &self.inner,
                self.plan.export_id(),
                &self.destination_root,
                &self.destination_relative,
            )?;
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
    // Byte-exact spelling comparison (ADR-004 §6): component-normalized
    // Path equality silently admits interior `.` and duplicate-separator
    // spellings, so the canonical form must equal the registered root
    // exactly, byte for byte.
    if canonical.as_os_str() != root.as_os_str() {
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
    let total = previous.checked_add(bytes).ok_or_else(|| {
        inner
            .export_staging_bytes
            .fetch_sub(bytes, Ordering::SeqCst);
        StorageError::ArithmeticOverflow("export staging bytes")
    })?;
    if total > MAX_EXPORT_TEMP_BYTES {
        inner
            .export_staging_bytes
            .fetch_sub(bytes, Ordering::SeqCst);
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
    let manifest = ExportManifest::decode(&manifest_json)?;
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
    let manifest = ExportManifest::decode(&manifest_json)?;
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::time::Duration;

    use arrow_array::{Int64Array, RecordBatch};
    use rusqlite::Connection;
    use sha2::{Digest, Sha256};
    use tempfile::TempDir;

    use stillflow_core::{
        logical_schema_to_arrow, BatchEnvelope, ColumnId, ExportDestination, ExportFormat,
        ExportInputIdentity, ExportPolicy, ExportShape, LogicalField, LogicalSchema, LogicalType,
        EXPORT_ENCODER_VERSION, EXPORT_FORMAT_CONTRACT_VERSION, EXPORT_JSONL_FLOAT_ENCODER,
        EXPORT_MANIFEST_VERSION, EXPORT_TEXT_FLOAT_ENCODER, MAX_ACTIVE_EXPORT_PUBLISHERS,
        MAX_EXPORT_PARTITIONS, MAX_EXPORT_ROWS, MAX_EXPORT_SINGLE_FILE_BYTES,
        MAX_EXPORT_TEMP_BYTES,
    };

    use crate::{
        SnapshotDraft, SnapshotManifest, SnapshotStore, StorageError, StorageLimits,
        STORAGE_SCHEMA_VERSION,
    };

    use super::*;

    fn at(second: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(second, 0).expect("valid timestamp")
    }

    fn logical_schema() -> std::sync::Arc<LogicalSchema> {
        std::sync::Arc::new(
            LogicalSchema::new(vec![LogicalField::new(
                ColumnId::from_uuid(Uuid::from_u128(11)),
                "value",
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
        schema: std::sync::Arc<LogicalSchema>,
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
        temp: &TempDir,
        snapshot_id: Uuid,
        source_asset_id: Uuid,
        schema: std::sync::Arc<LogicalSchema>,
        partitions: Vec<Vec<i64>>,
    ) -> SnapshotManifest {
        let store = store(temp);
        let mut writer = store
            .begin_snapshot(
                draft(snapshot_id, source_asset_id, &schema),
                at(1_700_000_001),
            )
            .expect("begin snapshot");
        for (sequence, values) in partitions.into_iter().enumerate() {
            writer
                .append(&envelope(
                    std::sync::Arc::clone(&schema),
                    source_asset_id,
                    u64::try_from(sequence).expect("test sequence"),
                    values,
                ))
                .expect("append envelope");
        }
        writer.commit().expect("commit snapshot")
    }

    /// Creates the Allowed Root of a test destination.
    fn destination_root(temp: &TempDir) -> PathBuf {
        let root = temp.path().join("published");
        std::fs::create_dir_all(&root).expect("destination root");
        root
    }

    fn input_identity(manifest: &SnapshotManifest) -> ExportInputIdentity {
        let snapshot = manifest.snapshot();
        ExportInputIdentity::try_new(
            snapshot.id(),
            snapshot.dataset_id(),
            snapshot.session_id(),
            snapshot.source_asset_id(),
            snapshot.schema_fingerprint(),
            snapshot.version(),
        )
        .expect("input identity")
    }

    fn local_plan(
        export_id: Uuid,
        manifest: &SnapshotManifest,
        root: &Path,
        relative: &[&str],
        format: ExportFormat,
        shape: ExportShape,
    ) -> ExportPlan {
        ExportPlan::try_new(
            export_id,
            input_identity(manifest),
            ExportDestination::local(
                root,
                relative.iter().map(|part| (*part).to_owned()).collect(),
                format,
                shape,
            )
            .expect("local destination"),
            format,
            ExportPolicy { shape },
        )
        .expect("export plan")
    }

    fn provenance(row_count: u64, created_at: DateTime<Utc>) -> ExportProvenance {
        ExportProvenance {
            created_at,
            row_count,
            engine_contract_version: 7,
        }
    }

    /// Installs one staged file with the given bytes.
    fn install_bytes(
        writer: &mut ExportWriter,
        bytes: &[u8],
    ) -> Result<ExportManifestFile, StorageError> {
        let mut staged = writer.create_staged_file()?;
        staged.write_bytes(bytes)?;
        writer.install_staged_file(staged)
    }

    fn hex_lower(bytes: &[u8]) -> String {
        use std::fmt::Write as _;
        let mut out = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            let _ = write!(&mut out, "{byte:02x}");
        }
        out
    }

    fn sha256_hex(bytes: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        hex_lower(&hasher.finalize())
    }

    fn connection(temp: &TempDir) -> Connection {
        Connection::open(temp.path().join("metadata.sqlite3")).expect("open metadata database")
    }

    fn export_journal_count(temp: &TempDir, export_id: Uuid) -> i64 {
        let connection = connection(temp);
        connection
            .query_row(
                "SELECT COUNT(*) FROM export_journal WHERE export_id = ?1",
                [export_id.to_string()],
                |row| row.get(0),
            )
            .expect("journal count")
    }

    fn export_publication_count(temp: &TempDir, export_id: Uuid) -> i64 {
        let connection = connection(temp);
        connection
            .query_row(
                "SELECT COUNT(*) FROM export_publications WHERE export_id = ?1",
                [export_id.to_string()],
                |row| row.get(0),
            )
            .expect("publication count")
    }

    fn staging_dir(temp: &TempDir, export_id: Uuid) -> PathBuf {
        temp.path()
            .join("export-staging")
            .join(export_id.to_string())
    }

    fn artifact_path(root: &Path, relative: &[&str]) -> PathBuf {
        let mut path = root.to_path_buf();
        for component in relative {
            path.push(component);
        }
        path
    }

    const REPORTS: [&str; 2] = ["reports", "sales.csv"];
    const SALES_SET: [&str; 2] = ["reports", "sales"];

    // -------------------------------------------------------------------------
    // Item 13/14: digests, manifest shape, provenance, secret absence
    // -------------------------------------------------------------------------

    #[test]
    fn export_manifest_records_exact_provenance_and_recomputable_digests() {
        let temp = TempDir::new().expect("temp directory");
        let source = Uuid::from_u128(4);
        let manifest = publish(
            &temp,
            Uuid::from_u128(1),
            source,
            logical_schema(),
            vec![vec![1, 2], vec![3]],
        );
        let root = destination_root(&temp);
        let export_id = Uuid::from_u128(100);
        let created_at = at(1_700_100_000);
        let bytes: &[u8] = b"value\n1,2\n3\n";

        let mut writer = store(&temp)
            .begin_export(
                local_plan(
                    export_id,
                    &manifest,
                    &root,
                    &REPORTS,
                    ExportFormat::Csv,
                    ExportShape::SingleFile,
                ),
                created_at,
            )
            .expect("begin export");
        let record = install_bytes(&mut writer, bytes).expect("install staged file");

        let committed = writer.commit(provenance(3, created_at)).expect("commit");

        assert_eq!(committed.export_id(), export_id);
        assert_eq!(committed.manifest_version(), EXPORT_MANIFEST_VERSION);
        assert_eq!(committed.format(), ExportFormat::Csv);
        assert_eq!(committed.shape(), ExportShape::SingleFile);
        assert_eq!(
            committed.format_contract_version(),
            EXPORT_FORMAT_CONTRACT_VERSION
        );
        assert_eq!(committed.encoder_version(), EXPORT_ENCODER_VERSION);
        assert_eq!(committed.jsonl_float_encoder(), EXPORT_JSONL_FLOAT_ENCODER);
        assert_eq!(committed.text_float_encoder(), EXPORT_TEXT_FLOAT_ENCODER);
        assert_eq!(committed.storage_schema_version(), STORAGE_SCHEMA_VERSION);
        assert_eq!(committed.engine_contract_version(), 7);
        assert_eq!(committed.created_at(), &created_at);
        assert_eq!(committed.row_count(), 3);
        assert_eq!(
            committed.byte_count(),
            u64::try_from(bytes.len()).expect("bytes")
        );
        assert_eq!(committed.files().len(), 1);
        assert_eq!(committed.files()[0].name(), "sales.csv");
        assert_eq!(
            committed.files()[0].byte_count(),
            u64::try_from(bytes.len()).expect("bytes")
        );
        assert_eq!(record.digest(), committed.files()[0].digest());
        assert_eq!(
            committed.files()[0].digest(),
            sha256_hex(bytes),
            "per-file digest must equal an independent SHA-256 over the file bytes"
        );
        assert_eq!(
            committed.set_digest(),
            sha256_hex(committed.files()[0].digest().as_bytes()),
            "set digest must equal SHA-256 over the LF-joined per-file digests"
        );
        assert_eq!(committed.destination_root(), root.as_path());
        assert_eq!(
            committed.destination_relative(),
            &["reports".to_owned(), "sales.csv".to_owned()][..]
        );

        // The destination bytes are exactly the staged bytes, and the stored
        // manifest row revalidates on every load.
        let written = std::fs::read(artifact_path(&root, &REPORTS)).expect("artifact bytes");
        assert_eq!(written, bytes);
        let loaded = store(&temp)
            .load_export_manifest(export_id)
            .expect("load manifest");
        assert_eq!(loaded, committed);
        assert_eq!(export_publication_count(&temp, export_id), 0);
        assert_eq!(export_journal_count(&temp, export_id), 0);
        assert!(!staging_dir(&temp, export_id).exists());

        // Manifests never contain cell values, secret material, or Debug
        // leakage (ADR-004 §7; AGENTS rule 10).
        let stored_json: String = connection(&temp)
            .query_row(
                "SELECT manifest_json FROM export_manifests WHERE export_id = ?1",
                [export_id.to_string()],
                |row| row.get(0),
            )
            .expect("manifest json");
        for sentinel in ["1,2", "value", "credential", "secret", "password"] {
            assert!(
                !stored_json.contains(sentinel),
                "manifest must not contain {sentinel:?}"
            );
        }
    }

    // -------------------------------------------------------------------------
    // Item 9/10/14/23: manifest revalidation fails closed
    // -------------------------------------------------------------------------

    #[test]
    fn export_manifest_revalidation_fails_closed_on_tampered_rows() {
        let temp = TempDir::new().expect("temp directory");
        let manifest = publish(
            &temp,
            Uuid::from_u128(1),
            Uuid::from_u128(4),
            logical_schema(),
            vec![vec![1], vec![2]],
        );
        let root = destination_root(&temp);
        let export_id = Uuid::from_u128(101);
        let created_at = at(1_700_100_000);

        // A partitioned commit gives the contiguity law real part names.
        let mut writer = store(&temp)
            .begin_export(
                local_plan(
                    export_id,
                    &manifest,
                    &root,
                    &SALES_SET,
                    ExportFormat::Csv,
                    ExportShape::PartitionedSet,
                ),
                created_at,
            )
            .expect("begin export");
        install_bytes(&mut writer, b"a\n").expect("install part 0");
        install_bytes(&mut writer, b"b\n").expect("install part 1");
        let committed = writer.commit(provenance(2, created_at)).expect("commit");
        assert_eq!(
            committed
                .files()
                .iter()
                .map(|file| file.name())
                .collect::<Vec<_>>(),
            ["part-0000000000.csv", "part-0000000001.csv"]
        );

        let store = store(&temp);
        let original: String = connection(&temp)
            .query_row(
                "SELECT manifest_json FROM export_manifests WHERE export_id = ?1",
                [export_id.to_string()],
                |row| row.get(0),
            )
            .expect("manifest json");
        store
            .load_export_manifest(export_id)
            .expect("positive control");

        let restore = |json: &str| {
            connection(&temp)
                .execute(
                    "UPDATE export_manifests SET manifest_json = ?1 WHERE export_id = ?2",
                    rusqlite::params![json, export_id.to_string()],
                )
                .expect("restore manifest row");
        };

        // Unknown/future manifest versions fail closed (item 23).
        let tampered = original.replace(
            &format!("\"manifestVersion\":{EXPORT_MANIFEST_VERSION}"),
            "\"manifestVersion\":2",
        );
        assert_ne!(tampered, original);
        restore(&tampered);
        assert!(matches!(
            store.load_export_manifest(export_id),
            Err(StorageError::UnsupportedStorageVersion(2))
        ));
        restore(&original);

        // A stored set digest that no longer matches the per-file digests.
        let tampered = original.replace(committed.set_digest(), &"0".repeat(64));
        restore(&tampered);
        assert!(matches!(
            store.load_export_manifest(export_id),
            Err(StorageError::InvalidManifest(
                "export set digest does not match the per-file digests"
            ))
        ));
        restore(&original);

        // Non-contiguous part names fail the partitioned-name law.
        let tampered = original.replace("part-0000000001.csv", "part-0000000002.csv");
        restore(&tampered);
        assert!(matches!(
            store.load_export_manifest(export_id),
            Err(StorageError::InvalidManifest(
                "partitioned export file names are not contiguous part names"
            ))
        ));
        restore(&original);

        // Row totals above MAX_EXPORT_ROWS fail typed.
        let tampered = original.replace(
            "\"rowCount\":2",
            &format!("\"rowCount\":{}", MAX_EXPORT_ROWS + 1),
        );
        restore(&tampered);
        assert!(matches!(
            store.load_export_manifest(export_id),
            Err(StorageError::ExportLimitExceeded {
                resource: "export rows",
                ..
            })
        ));
        restore(&original);

        // One file above MAX_EXPORT_SINGLE_FILE_BYTES fails typed.
        let tampered = original.replace(
            "\"byteCount\":2",
            &format!("\"byteCount\":{}", MAX_EXPORT_SINGLE_FILE_BYTES + 1),
        );
        restore(&tampered);
        assert!(matches!(
            store.load_export_manifest(export_id),
            Err(StorageError::ExportLimitExceeded {
                resource: "export single file bytes",
                ..
            })
        ));
        restore(&original);

        // A nil export identity fails closed.
        let tampered = original.replace(
            &format!("\"exportId\":\"{export_id}\""),
            "\"exportId\":\"00000000-0000-0000-0000-000000000000\"",
        );
        restore(&tampered);
        assert!(matches!(
            store.load_export_manifest(export_id),
            Err(StorageError::InvalidManifest(
                "export identity must not be nil"
            ))
        ));
        restore(&original);

        // A set fanned out beyond MAX_EXPORT_PARTITIONS fails typed. The
        // manifest is crafted directly because no writer can produce it.
        let mut files = Vec::new();
        let mut digests = Vec::new();
        for sequence in 0..=MAX_EXPORT_PARTITIONS {
            let digest = format!("{:064x}", sequence);
            files.push(format!(
                "{{\"name\":\"part-{sequence:010}.csv\",\"byteCount\":1,\"digest\":\"{digest}\"}}"
            ));
            digests.push(digest);
        }
        let crafted = format!(
            "{{\"manifestVersion\":1,\"exportId\":\"{export_id}\",\"input\":{},\"format\":\"csv\",\"shape\":\"partitionedSet\",\"formatContractVersion\":1,\"encoderVersion\":{},\"jsonlFloatEncoder\":{},\"textFloatEncoder\":{},\"storageSchemaVersion\":1,\"engineContractVersion\":7,\"createdAt\":\"2023-11-14T22:13:20Z\",\"rowCount\":1,\"byteCount\":1025,\"files\":[{}],\"setDigest\":\"{}\",\"destinationRoot\":{},\"destinationRelative\":[\"reports\",\"sales\"]}}",
            serde_json::to_string(&committed.input()).expect("input"),
            serde_json::to_string(EXPORT_ENCODER_VERSION).expect("encoder"),
            serde_json::to_string(EXPORT_JSONL_FLOAT_ENCODER).expect("jsonl encoder"),
            serde_json::to_string(EXPORT_TEXT_FLOAT_ENCODER).expect("text encoder"),
            files.join(","),
            compute_export_set_digest(&digests),
            serde_json::to_string(&root).expect("root"),
        );
        restore(&crafted);
        assert!(matches!(
            store.load_export_manifest(export_id),
            Err(StorageError::ExportLimitExceeded {
                resource: "export partitions",
                ..
            })
        ));
        restore(&original);
        store
            .load_export_manifest(export_id)
            .expect("restored manifest");
    }

    // -------------------------------------------------------------------------
    // Item 12: strict create-new publication and collisions
    // -------------------------------------------------------------------------

    #[test]
    fn publication_is_strictly_create_new_and_never_overwrites() {
        let temp = TempDir::new().expect("temp directory");
        let manifest = publish(
            &temp,
            Uuid::from_u128(1),
            Uuid::from_u128(4),
            logical_schema(),
            vec![vec![1]],
        );
        let root = destination_root(&temp);
        let created_at = at(1_700_100_000);

        // Existing file at a single-file destination.
        std::fs::create_dir_all(root.join("reports")).expect("destination parent");
        let file_destination = artifact_path(&root, &REPORTS);
        std::fs::write(&file_destination, b"existing\n").expect("existing file");
        assert!(matches!(
            store(&temp).begin_export(
                local_plan(
                    Uuid::from_u128(200),
                    &manifest,
                    &root,
                    &REPORTS,
                    ExportFormat::Csv,
                    ExportShape::SingleFile,
                ),
                created_at,
            ),
            Err(StorageError::ExportDestinationExists(_))
        ));
        assert_eq!(
            std::fs::read(&file_destination).expect("existing bytes"),
            b"existing\n",
            "the existing destination file must be untouched"
        );
        std::fs::remove_file(&file_destination).expect("remove existing file");

        // Existing directory at a single-file destination.
        std::fs::create_dir_all(&file_destination).expect("existing directory");
        assert!(matches!(
            store(&temp).begin_export(
                local_plan(
                    Uuid::from_u128(201),
                    &manifest,
                    &root,
                    &REPORTS,
                    ExportFormat::Csv,
                    ExportShape::SingleFile,
                ),
                created_at,
            ),
            Err(StorageError::ExportDestinationExists(_))
        ));
        std::fs::remove_dir(&file_destination).expect("remove existing directory");

        // Existing directory at a partitioned-set destination.
        let set_directory = artifact_path(&root, &SALES_SET);
        std::fs::create_dir_all(&set_directory).expect("existing set directory");
        assert!(matches!(
            store(&temp).begin_export(
                local_plan(
                    Uuid::from_u128(202),
                    &manifest,
                    &root,
                    &SALES_SET,
                    ExportFormat::Csv,
                    ExportShape::PartitionedSet,
                ),
                created_at,
            ),
            Err(StorageError::ExportDestinationExists(_))
        ));
        std::fs::remove_dir(&set_directory).expect("remove set directory");

        // A live export id cannot be claimed twice.
        let active_store = store(&temp);
        let first = active_store
            .begin_export(
                local_plan(
                    Uuid::from_u128(203),
                    &manifest,
                    &root,
                    &REPORTS,
                    ExportFormat::Csv,
                    ExportShape::SingleFile,
                ),
                created_at,
            )
            .expect("first begin");
        assert!(matches!(
            active_store.begin_export(
                local_plan(
                    Uuid::from_u128(203),
                    &manifest,
                    &root,
                    &SALES_SET,
                    ExportFormat::Csv,
                    ExportShape::PartitionedSet,
                ),
                created_at,
            ),
            Err(StorageError::AlreadyExists(_))
        ));

        // A destination file created between begin and install fails
        // create-new at the rename precheck.
        let mut raced = active_store
            .begin_export(
                local_plan(
                    Uuid::from_u128(204),
                    &manifest,
                    &root,
                    &REPORTS,
                    ExportFormat::Csv,
                    ExportShape::SingleFile,
                ),
                created_at,
            )
            .expect("raced begin");
        std::fs::write(&file_destination, b"raced\n").expect("raced file");
        let staged = raced.create_staged_file().expect("staged file");
        assert!(matches!(
            raced.install_staged_file(staged),
            Err(StorageError::ExportDestinationExists(_))
        ));
        assert_eq!(
            std::fs::read(&file_destination).expect("raced bytes"),
            b"raced\n",
            "the raced destination file must be untouched"
        );
        drop(raced);
        std::fs::remove_file(&file_destination).expect("remove raced file");

        // A set directory created between begin and first install fails
        // create-new at the set-directory materialization.
        let mut set_writer = active_store
            .begin_export(
                local_plan(
                    Uuid::from_u128(205),
                    &manifest,
                    &root,
                    &SALES_SET,
                    ExportFormat::Csv,
                    ExportShape::PartitionedSet,
                ),
                created_at,
            )
            .expect("set begin");
        std::fs::create_dir_all(&set_directory).expect("raced set directory");
        let staged = set_writer.create_staged_file().expect("staged part");
        assert!(matches!(
            set_writer.install_staged_file(staged),
            Err(StorageError::ExportDestinationExists(_))
        ));
        drop(set_writer);
        std::fs::remove_dir_all(&set_directory).expect("remove raced set directory");

        // A staged file collision is a typed failure, never a merge.
        let mut colliding = active_store
            .begin_export(
                local_plan(
                    Uuid::from_u128(206),
                    &manifest,
                    &root,
                    &REPORTS,
                    ExportFormat::Csv,
                    ExportShape::SingleFile,
                ),
                created_at,
            )
            .expect("colliding begin");
        std::fs::write(
            staging_dir(&temp, Uuid::from_u128(206)).join("0000000000.staged"),
            b"residue\n",
        )
        .expect("planted staged file");
        assert!(matches!(
            colliding.create_staged_file(),
            Err(StorageError::ExportStagingExists(_))
        ));
        drop(colliding);

        // After the earlier writer is dropped (graceful abort), the id is
        // free again and a fresh publication succeeds end to end.
        drop(first);
        assert_eq!(export_publication_count(&temp, Uuid::from_u128(203)), 0);
        assert!(!staging_dir(&temp, Uuid::from_u128(203)).exists());
        let mut retried = active_store
            .begin_export(
                local_plan(
                    Uuid::from_u128(203),
                    &manifest,
                    &root,
                    &REPORTS,
                    ExportFormat::Csv,
                    ExportShape::SingleFile,
                ),
                created_at,
            )
            .expect("retried begin");
        install_bytes(&mut retried, b"final\n").expect("install after retry");
        let committed = retried
            .commit(provenance(1, created_at))
            .expect("retry commit");
        assert_eq!(committed.row_count(), 1);
    }

    // -------------------------------------------------------------------------
    // Item 11: destination-root filesystem safety
    // -------------------------------------------------------------------------

    #[test]
    fn destination_root_rejects_symlinks_non_directories_and_non_canonical_paths() {
        let temp = TempDir::new().expect("temp directory");
        let manifest = publish(
            &temp,
            Uuid::from_u128(1),
            Uuid::from_u128(4),
            logical_schema(),
            vec![vec![1]],
        );
        let created_at = at(1_700_100_000);
        let begin = |root: &Path| {
            store(&temp).begin_export(
                local_plan(
                    Uuid::from_u128(300),
                    &manifest,
                    root,
                    &REPORTS,
                    ExportFormat::Csv,
                    ExportShape::SingleFile,
                ),
                created_at,
            )
        };

        // A missing root fails typed before any byte is written.
        let missing = temp.path().join("missing-root");
        assert!(matches!(begin(&missing), Err(StorageError::Io { .. })));

        // A regular-file root is rejected.
        let file_root = temp.path().join("file-root");
        std::fs::write(&file_root, b"not a directory").expect("file root");
        assert!(matches!(
            begin(&file_root),
            Err(StorageError::InvalidConfiguration(
                "export destination root must be a non-symlink directory",
            ))
        ));

        // A symlinked root is rejected (no-follow).
        let real = temp.path().join("real-root");
        std::fs::create_dir_all(&real).expect("real root");
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&real, temp.path().join("linked-root"))
                .expect("symlink root");
            assert!(matches!(
                begin(&temp.path().join("linked-root")),
                Err(StorageError::InvalidConfiguration(
                    "export destination root must be a non-symlink directory",
                ))
            ));
        }

        // A non-canonical spelling of an existing directory is rejected so
        // comparisons stay byte-exact at the contract layer.
        let dot_path = temp.path().join(".").join("real-root");
        assert!(matches!(
            begin(&dot_path),
            Err(StorageError::InvalidConfiguration(
                "export destination root must be its canonical non-symlink form",
            ))
        ));

        // The canonical non-symlink directory is accepted.
        let writer = begin(&real).expect("canonical root accepted");
        drop(writer);
    }

    // -------------------------------------------------------------------------
    // Item 15: journal precedes rename; commit is the visibility point
    // -------------------------------------------------------------------------

    #[test]
    fn journal_precedes_rename_and_manifest_commit_is_the_visibility_point() {
        let temp = TempDir::new().expect("temp directory");
        let manifest = publish(
            &temp,
            Uuid::from_u128(1),
            Uuid::from_u128(4),
            logical_schema(),
            vec![vec![1], vec![2]],
        );
        let root = destination_root(&temp);
        let export_id = Uuid::from_u128(400);
        let created_at = at(1_700_100_000);
        let export_store = store(&temp);

        let mut writer = export_store
            .begin_export(
                local_plan(
                    export_id,
                    &manifest,
                    &root,
                    &SALES_SET,
                    ExportFormat::Csv,
                    ExportShape::PartitionedSet,
                ),
                created_at,
            )
            .expect("begin export");

        // Not visible before any install.
        assert!(export_store.load_export_manifest(export_id).is_err());

        install_bytes(&mut writer, b"part-zero\n").expect("install part 0");
        // The journal rows exist before the rename left the file behind: one
        // for the part path and one for the materialized set directory.
        assert_eq!(export_journal_count(&temp, export_id), 2);
        assert!(artifact_path(&root, &["reports", "sales", "part-0000000000.csv"]).exists());
        // Installed files are still invisible: no manifest yet.
        assert!(export_store.load_export_manifest(export_id).is_err());

        install_bytes(&mut writer, b"part-one\n").expect("install part 1");
        assert_eq!(export_journal_count(&temp, export_id), 3);
        assert_eq!(export_publication_count(&temp, export_id), 1);
        assert!(export_store.load_export_manifest(export_id).is_err());

        // The manifest commit is the single visibility point and clears the
        // durable journals.
        writer.commit(provenance(2, created_at)).expect("commit");
        assert_eq!(export_journal_count(&temp, export_id), 0);
        assert_eq!(export_publication_count(&temp, export_id), 0);
        assert!(export_store.load_export_manifest(export_id).is_ok());
        assert!(artifact_path(&root, &["reports", "sales", "part-0000000000.csv"]).exists());
        assert!(artifact_path(&root, &["reports", "sales", "part-0000000001.csv"]).exists());
    }

    // -------------------------------------------------------------------------
    // Item 15/16: crash windows, recovery completeness, idempotence
    // -------------------------------------------------------------------------

    /// Plants a crash-window state directly: one stale publication claim, a
    /// staging directory, and `journaled_parts` destination files that were
    /// already renamed before the (simulated) crash. The journal rows mirror
    /// the writer discipline: every part path plus the set directory itself.
    fn plant_crash(temp: &TempDir, export_id: Uuid, root: &Path, journaled_parts: &[&str]) {
        let connection = connection(temp);
        let journal = |relative: &[&str]| {
            connection
                .execute(
                    "INSERT INTO export_journal(
                         export_id, destination_root, destination_relative, journaled_at_utc
                     ) VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![
                        export_id.to_string(),
                        root.to_string_lossy(),
                        serde_json::to_string(relative).expect("relative"),
                        format_timestamp(&at(1_700_000_600)),
                    ],
                )
                .expect("plant journal row");
        };
        connection
            .execute(
                "INSERT INTO export_publications(
                     export_id, snapshot_id, destination_root, destination_relative, started_at_utc
                 ) VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    export_id.to_string(),
                    Uuid::from_u128(1).to_string(),
                    root.to_string_lossy(),
                    serde_json::to_string(&["reports", "sales"]).expect("relative"),
                    format_timestamp(&at(1_700_000_500)),
                ],
            )
            .expect("plant publication row");
        std::fs::create_dir_all(staging_dir(temp, export_id)).expect("plant staging");
        std::fs::write(
            staging_dir(temp, export_id).join("0000000000.staged"),
            b"staged",
        )
        .expect("plant staged bytes");
        for part in journaled_parts {
            journal(&["reports", "sales", part]);
            let path = artifact_path(root, &["reports", "sales", part]);
            std::fs::create_dir_all(path.parent().expect("parent")).expect("set directory");
            std::fs::write(&path, b"renamed-before-crash\n").expect("plant destination file");
        }
        journal(&["reports", "sales"]);
    }

    #[test]
    fn recovery_cleans_every_crash_window_and_never_publishes_or_deletes_committed_artifacts() {
        let temp = TempDir::new().expect("temp directory");
        let manifest = publish(
            &temp,
            Uuid::from_u128(1),
            Uuid::from_u128(4),
            logical_schema(),
            vec![vec![1]],
        );
        let root = destination_root(&temp);
        let now = at(1_700_100_000);

        // Window A: crash before the first rename (publication + staging only).
        let before_rename = Uuid::from_u128(500);
        plant_crash(&temp, before_rename, &root, &[]);
        // Window B: crash mid multi-file rename (part 0 renamed, part 1 not).
        let mid_rename = Uuid::from_u128(501);
        plant_crash(&temp, mid_rename, &root, &["part-0000000000.csv"]);
        // Window C: crash after all renames, before the manifest commit.
        let after_renames = Uuid::from_u128(502);
        plant_crash(
            &temp,
            after_renames,
            &root,
            &["part-0000000000.csv", "part-0000000001.csv"],
        );

        let report = store(&temp)
            .recover(now, Duration::from_secs(60), 16)
            .expect("recovery");
        assert!(report.examined() >= 3, "all three windows must be examined");

        for (export_id, parts) in [(before_rename, 0), (mid_rename, 1), (after_renames, 2)] {
            assert_eq!(export_publication_count(&temp, export_id), 0);
            assert_eq!(export_journal_count(&temp, export_id), 0);
            assert!(!staging_dir(&temp, export_id).exists());
            for sequence in 0..parts {
                let part = format!("part-{sequence:010}.csv");
                assert!(
                    !artifact_path(&root, &["reports", "sales", &part]).exists(),
                    "renamed pre-publication residue {part} must be deleted by recovery"
                );
            }
            assert!(
                !artifact_path(&root, &SALES_SET).exists(),
                "the journaled set directory must be deleted so the name is free"
            );
            // Recovery never publishes: no manifest row may appear.
            assert!(store(&temp).load_export_manifest(export_id).is_err());
        }

        // Window D: orphan journal rows of a gracefully aborted writer (no
        // publication claim) plus an orphan staging directory.
        let aborted = Uuid::from_u128(503);
        let db = connection(&temp);
        db.execute(
            "INSERT INTO export_journal(
                     export_id, destination_root, destination_relative, journaled_at_utc
                 ) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![
                aborted.to_string(),
                root.to_string_lossy(),
                serde_json::to_string(&["reports", "sales.csv"]).expect("relative"),
                format_timestamp(&at(1_700_000_700)),
            ],
        )
        .expect("plant orphan journal row");
        drop(db);
        std::fs::create_dir_all(artifact_path(&root, &["reports"])).expect("parent");
        std::fs::write(artifact_path(&root, &REPORTS), b"residue\n").expect("orphan destination");
        let orphan_staging = Uuid::from_u128(504);
        std::fs::create_dir_all(staging_dir(&temp, orphan_staging)).expect("orphan staging");

        store(&temp)
            .recover(now, Duration::from_secs(60), 16)
            .expect("second recovery");
        assert_eq!(export_journal_count(&temp, aborted), 0);
        assert!(!artifact_path(&root, &REPORTS).exists());
        assert!(!staging_dir(&temp, orphan_staging).exists());

        // Window E: a committed manifest with stale claim rows must never be
        // deleted. The artifact bytes survive and the manifest stays visible.
        let committed_id = Uuid::from_u128(505);
        let mut writer = store(&temp)
            .begin_export(
                local_plan(
                    committed_id,
                    &manifest,
                    &root,
                    &REPORTS,
                    ExportFormat::Csv,
                    ExportShape::SingleFile,
                ),
                at(1_700_050_000),
            )
            .expect("committed begin");
        let committed_bytes: &[u8] = b"committed artifact\n";
        install_bytes(&mut writer, committed_bytes).expect("committed install");
        writer
            .commit(provenance(1, at(1_700_050_000)))
            .expect("commit");
        // Simulate a lingering claim and staging residue around the commit.
        connection(&temp)
            .execute(
                "INSERT INTO export_publications(
                     export_id, snapshot_id, destination_root, destination_relative, started_at_utc
                 ) VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    committed_id.to_string(),
                    Uuid::from_u128(1).to_string(),
                    root.to_string_lossy(),
                    serde_json::to_string(&REPORTS).expect("relative"),
                    format_timestamp(&at(1_700_000_500)),
                ],
            )
            .expect("plant stale claim");
        std::fs::create_dir_all(staging_dir(&temp, committed_id)).expect("plant committed staging");

        store(&temp)
            .recover(now, Duration::from_secs(60), 16)
            .expect("recovery over committed case");
        assert_eq!(
            std::fs::read(artifact_path(&root, &REPORTS)).expect("artifact survives"),
            committed_bytes,
            "recovery must never delete a manifest-committed artifact"
        );
        assert!(store(&temp).load_export_manifest(committed_id).is_ok());
        assert_eq!(export_publication_count(&temp, committed_id), 0);
        assert!(!staging_dir(&temp, committed_id).exists());
    }

    #[test]
    fn recovery_is_idempotent_and_frees_identity_and_destination_names() {
        let temp = TempDir::new().expect("temp directory");
        let manifest = publish(
            &temp,
            Uuid::from_u128(1),
            Uuid::from_u128(4),
            logical_schema(),
            vec![vec![1]],
        );
        let root = destination_root(&temp);
        let export_id = Uuid::from_u128(600);
        let now = at(1_700_100_000);
        plant_crash(
            &temp,
            export_id,
            &root,
            &["part-0000000000.csv", "part-0000000001.csv"],
        );

        let first = store(&temp)
            .recover(now, Duration::from_secs(60), 16)
            .expect("first recovery");
        assert!(first.recovered() >= 1);
        let second = store(&temp)
            .recover(now, Duration::from_secs(60), 16)
            .expect("second recovery");
        assert_eq!(
            second.recovered(),
            0,
            "a repeated recovery over cleaned state must be a no-op"
        );
        assert_eq!(export_publication_count(&temp, export_id), 0);

        // The id and the destination name are publishable again only after
        // the uncommitted cleanup, and the retry commits end to end.
        let mut retried = store(&temp)
            .begin_export(
                local_plan(
                    export_id,
                    &manifest,
                    &root,
                    &SALES_SET,
                    ExportFormat::Csv,
                    ExportShape::PartitionedSet,
                ),
                at(1_700_100_100),
            )
            .expect("id and destination free after recovery");
        install_bytes(&mut retried, b"retry-0\n").expect("retry part 0");
        install_bytes(&mut retried, b"retry-1\n").expect("retry part 1");
        retried
            .commit(provenance(2, at(1_700_100_100)))
            .expect("retry commit");
        assert!(store(&temp).load_export_manifest(export_id).is_ok());
    }

    // -------------------------------------------------------------------------
    // Item 17/21: gates, concurrency caps, tombstone-first retention
    // -------------------------------------------------------------------------

    #[test]
    fn maintenance_and_publisher_gates_bound_exports() {
        let temp = TempDir::new().expect("temp directory");
        let manifest = publish(
            &temp,
            Uuid::from_u128(1),
            Uuid::from_u128(4),
            logical_schema(),
            vec![vec![1]],
        );
        let root = destination_root(&temp);
        let now = at(1_700_100_000);
        let plan = |export_id: u128, relative: &'static [&'static str], shape| {
            local_plan(
                Uuid::from_u128(export_id),
                &manifest,
                &root,
                relative,
                ExportFormat::Csv,
                shape,
            )
        };

        let active = store(&temp);
        let writers: Vec<_> = (0..MAX_ACTIVE_EXPORT_PUBLISHERS)
            .map(|index| {
                let export_id = u128::from(700 + index);
                let (relative, shape): (&'static [&'static str], ExportShape) = if index % 2 == 0 {
                    (&REPORTS, ExportShape::SingleFile)
                } else {
                    (&SALES_SET, ExportShape::PartitionedSet)
                };
                active
                    .begin_export(plan(export_id, relative, shape), at(1_700_090_000))
                    .expect("concurrent export begin")
            })
            .collect();
        assert_eq!(writers.len(), usize::from(MAX_ACTIVE_EXPORT_PUBLISHERS));

        // The fifth concurrent publication fails the frozen cap.
        assert!(matches!(
            active.begin_export(
                plan(800, &REPORTS, ExportShape::SingleFile),
                at(1_700_090_000)
            ),
            Err(StorageError::Busy("active export publisher limit reached"))
        ));

        // Maintenance excludes live export publishers (and vice versa).
        assert!(matches!(
            active.recover(now, Duration::from_secs(60), 16),
            Err(StorageError::Busy(_))
        ));
        assert!(matches!(
            active.collect_garbage(now, Duration::from_secs(0), 16),
            Err(StorageError::Busy(_))
        ));
        drop(writers);
        active
            .recover(now, Duration::from_secs(60), 16)
            .expect("maintenance after exports drain");
    }

    #[test]
    fn tombstone_hides_exports_and_gc_collects_only_after_retention_cutoff() {
        let temp = TempDir::new().expect("temp directory");
        let manifest = publish(
            &temp,
            Uuid::from_u128(1),
            Uuid::from_u128(4),
            logical_schema(),
            vec![vec![1]],
        );
        let root = destination_root(&temp);
        let export_id = Uuid::from_u128(900);
        let created_at = at(1_700_090_000);
        let artifact = artifact_path(&root, &REPORTS);

        let mut writer = store(&temp)
            .begin_export(
                local_plan(
                    export_id,
                    &manifest,
                    &root,
                    &REPORTS,
                    ExportFormat::Csv,
                    ExportShape::SingleFile,
                ),
                created_at,
            )
            .expect("begin export");
        install_bytes(&mut writer, b"tombstone me\n").expect("install");
        writer.commit(provenance(1, created_at)).expect("commit");

        // The manifest commit stamps the real wall clock; tombstone instants
        // are chosen relative to it.
        let committed_at = Utc::now();
        let tombstoned_at = committed_at + chrono::Duration::seconds(1);

        // A tombstone before the commit instant fails typed.
        assert!(matches!(
            store(&temp).tombstone_export(export_id, at(1)),
            Err(StorageError::InvalidTimestampOrder(
                "export commit and tombstone"
            ))
        ));

        // Tombstoning hides the manifest; the id stays claimed and the bytes
        // remain at the destination.
        store(&temp)
            .tombstone_export(export_id, tombstoned_at)
            .expect("tombstone");
        assert!(matches!(
            store(&temp).load_export_manifest(export_id),
            Err(StorageError::NotFound(_))
        ));
        assert!(
            matches!(
                store(&temp).begin_export(
                    local_plan(
                        export_id,
                        &manifest,
                        &root,
                        &SALES_SET,
                        ExportFormat::Csv,
                        ExportShape::PartitionedSet,
                    ),
                    at(1_700_100_100),
                ),
                Err(StorageError::AlreadyExists(_))
            ),
            "a tombstoned id stays claimed until collection"
        );
        assert!(
            matches!(
                store(&temp).begin_export(
                    local_plan(
                        Uuid::from_u128(901),
                        &manifest,
                        &root,
                        &REPORTS,
                        ExportFormat::Csv,
                        ExportShape::SingleFile,
                    ),
                    at(1_700_100_100),
                ),
                Err(StorageError::ExportDestinationExists(_))
            ),
            "the destination of an uncollected tombstone stays taken"
        );
        assert!(artifact.exists());

        // Garbage collection before the retention cutoff retains bytes.
        let early = store(&temp)
            .collect_garbage(
                tombstoned_at + chrono::Duration::seconds(200),
                Duration::from_secs(1_000),
                16,
            )
            .expect("early gc");
        assert_eq!(early.deleted(), 0, "nothing is collected before the cutoff");
        assert!(artifact.exists(), "bytes survive the cutoff");

        // Garbage collection after the explicit cutoff deletes the bytes and
        // frees both the tombstone and the destination name.
        let report = store(&temp)
            .collect_garbage(
                tombstoned_at + chrono::Duration::seconds(1_200),
                Duration::from_secs(1_000),
                16,
            )
            .expect("cutoff gc");
        assert!(report.deleted() >= 1);
        assert!(!artifact.exists(), "collected bytes are gone");
        assert_eq!(export_publication_count(&temp, export_id), 0);
        let connection = connection(&temp);
        let tombstones: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM export_tombstones WHERE export_id = ?1",
                [export_id.to_string()],
                |row| row.get(0),
            )
            .expect("tombstone count");
        drop(connection);
        assert_eq!(tombstones, 0);

        // The name is publishable again after collection.
        let mut fresh = store(&temp)
            .begin_export(
                local_plan(
                    Uuid::from_u128(902),
                    &manifest,
                    &root,
                    &REPORTS,
                    ExportFormat::Csv,
                    ExportShape::SingleFile,
                ),
                at(1_700_102_000),
            )
            .expect("name free after collection");
        install_bytes(&mut fresh, b"fresh\n").expect("fresh install");
        fresh
            .commit(provenance(1, at(1_700_102_000)))
            .expect("fresh commit");

        // Garbage collection never touches a visible export.
        let visible = store(&temp)
            .load_export_manifest(Uuid::from_u128(902))
            .expect("visible");
        store(&temp)
            .collect_garbage(at(1_700_200_000), Duration::from_secs(0), 16)
            .expect("gc over visible export");
        assert!(store(&temp)
            .load_export_manifest(Uuid::from_u128(902))
            .is_ok());
        assert_eq!(
            store(&temp)
                .load_export_manifest(Uuid::from_u128(902))
                .expect("still there"),
            visible
        );
        assert!(artifact.exists());
    }

    // -------------------------------------------------------------------------
    // Item 10: per-root staging budget
    // -------------------------------------------------------------------------

    #[test]
    fn staging_budget_is_per_root_and_fails_typed_above_the_ceiling() {
        let temp = TempDir::new().expect("temp directory");
        let manifest = publish(
            &temp,
            Uuid::from_u128(1),
            Uuid::from_u128(4),
            logical_schema(),
            vec![vec![1]],
        );
        let root = destination_root(&temp);
        let export_store = store(&temp);
        let export_id = Uuid::from_u128(1_000);
        let created_at = at(1_700_100_000);

        // Park the live staging counter just below the ceiling so a real
        // write crosses it without materializing 16 GiB.
        let headroom = 1_024_u64;
        export_store
            .inner
            .export_staging_bytes
            .store(MAX_EXPORT_TEMP_BYTES - headroom, Ordering::SeqCst);

        let mut writer = export_store
            .begin_export(
                local_plan(
                    export_id,
                    &manifest,
                    &root,
                    &REPORTS,
                    ExportFormat::Csv,
                    ExportShape::SingleFile,
                ),
                created_at,
            )
            .expect("begin export under budget");

        // Below the ceiling a bounded write succeeds and releases its bytes
        // with the staged handle.
        let mut staged = writer.create_staged_file().expect("first staged file");
        staged
            .write_bytes(&vec![
                b'a';
                usize::try_from(headroom / 2).expect("headroom")
            ])
            .expect("write under the ceiling");
        staged.flush_buffer().expect("flush under the ceiling");
        drop(staged);
        assert_eq!(
            export_store
                .inner
                .export_staging_bytes
                .load(Ordering::SeqCst),
            MAX_EXPORT_TEMP_BYTES - headroom
        );

        // Crossing the ceiling fails typed at the flush boundary and never
        // under-counts: the rejected delta is rolled back, leaving the live
        // budget exactly as it was before the rejected write.
        let mut staged = writer.create_staged_file().expect("second staged file");
        staged
            .write_bytes(&vec![
                b'b';
                usize::try_from(2 * headroom).expect("headroom")
            ])
            .expect("buffer accept");
        assert!(matches!(
            staged.flush_buffer(),
            Err(StorageError::ExportLimitExceeded {
                resource: "export staging bytes",
                ..
            })
        ));
        drop(staged);
        drop(writer);
        // The rejected delta is rolled back on failure, so the budget is
        // exactly where it was before the rejected write — no leaked count.
        assert_eq!(
            export_store
                .inner
                .export_staging_bytes
                .load(Ordering::SeqCst),
            MAX_EXPORT_TEMP_BYTES - headroom
        );
    }

    // -------------------------------------------------------------------------
    // Item 13: set-digest law
    // -------------------------------------------------------------------------

    #[test]
    fn set_digest_is_sha256_over_lf_joined_partition_order() {
        let one = "a".repeat(64);
        let two = "b".repeat(64);
        let three = "c".repeat(64);

        let joined = format!("{one}\n{two}\n{three}");
        assert_eq!(
            compute_export_set_digest([&one, &two, &three]),
            sha256_hex(joined.as_bytes())
        );
        // Order matters; the joined form has no trailing line feed.
        assert_ne!(
            compute_export_set_digest([&three, &two, &one]),
            compute_export_set_digest([&one, &two, &three])
        );
        assert_eq!(
            compute_export_set_digest([&one]),
            sha256_hex(one.as_bytes())
        );
        assert_eq!(
            compute_export_set_digest(Vec::<&str>::new()),
            sha256_hex(b"")
        );
    }

    // -------------------------------------------------------------------------
    // Item 11: symlinked components below the root; relative/home roots
    // -------------------------------------------------------------------------

    #[test]
    fn symlinked_components_below_the_root_fail_before_any_byte() {
        let temp = TempDir::new().expect("temp directory");
        let manifest = publish(
            &temp,
            Uuid::from_u128(1),
            Uuid::from_u128(4),
            logical_schema(),
            vec![vec![1]],
        );
        let root = destination_root(&temp);
        let created_at = at(1_700_100_000);

        // A destination parent that is a symlink is rejected by the
        // managed-directory no-follow discipline before any byte is written.
        let outside = temp.path().join("outside");
        std::fs::create_dir_all(&outside).expect("outside directory");
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&outside, root.join("reports")).expect("symlink parent");
            assert!(matches!(
                store(&temp).begin_export(
                    local_plan(
                        Uuid::from_u128(310),
                        &manifest,
                        &root,
                        &REPORTS,
                        ExportFormat::Csv,
                        ExportShape::SingleFile,
                    ),
                    created_at,
                ),
                Err(StorageError::InvalidConfiguration(
                    "managed entry must be a non-symlink directory",
                ))
            ));
            assert!(
                std::fs::read_dir(&outside)
                    .expect("outside listing")
                    .count()
                    == 0,
                "no byte may be written through the symlinked parent"
            );
        }

        // A home-style relative shortcut never reaches the store: the
        // destination value rejects non-absolute roots at construction.
        assert!(ExportDestination::local(
            "~",
            vec!["data.csv".to_owned()],
            ExportFormat::Csv,
            ExportShape::SingleFile,
        )
        .is_err());
    }

    // -------------------------------------------------------------------------
    // Items 11/15: corrupt journal metadata fails closed without wrong
    // deletion, and recovery resumes after the metadata is repaired
    // -------------------------------------------------------------------------

    #[test]
    fn corrupt_journal_rows_fail_closed_without_wrong_deletion() {
        let temp = TempDir::new().expect("temp directory");
        let manifest = publish(
            &temp,
            Uuid::from_u128(1),
            Uuid::from_u128(4),
            logical_schema(),
            vec![vec![1]],
        );
        let root = destination_root(&temp);
        let export_id = Uuid::from_u128(320);
        let now = at(1_700_100_000);
        let db = connection(&temp);
        let journal = |relative: &str, at_second: i64| {
            db.execute(
                "INSERT INTO export_journal(
                     export_id, destination_root, destination_relative, journaled_at_utc
                 ) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![
                    export_id.to_string(),
                    root.to_string_lossy(),
                    relative,
                    format_timestamp(&at(at_second)),
                ],
            )
            .expect("plant journal row");
        };

        // A traversal row names a path outside the root. Recovery must fail
        // closed instead of deleting anything, and the legitimate row behind
        // it must stay untouched.
        let traversal_row = serde_json::to_string(&["..", "escape.csv"]).expect("relative");
        journal(&traversal_row, 1);
        let legitimate = artifact_path(&root, &["inside.csv"]);
        std::fs::write(&legitimate, b"keep me\n").expect("legitimate residue");
        let legitimate_row = serde_json::to_string(&["inside.csv"]).expect("relative");
        journal(&legitimate_row, 2);
        assert!(matches!(
            store(&temp).recover(now, Duration::from_secs(60), 16),
            Err(StorageError::InvalidManifest(
                "export destination path is invalid",
            ))
        ));
        assert!(legitimate.exists(), "the row behind the corrupt one stays");
        assert!(
            store_dir_contents(&temp) > 0,
            "the store itself must be untouched by the failed sweep"
        );

        // A non-JSON path payload fails closed the same way.
        db.execute("DELETE FROM export_journal", [])
            .expect("clear rows");
        journal("not-json", 3);
        assert!(matches!(
            store(&temp).recover(now, Duration::from_secs(60), 16),
            Err(StorageError::InvalidManifest(
                "export journal path is invalid",
            ))
        ));

        // After the corrupt rows are repaired the sweep runs to completion
        // and cleans the legitimate residue (recovery is re-runnable).
        db.execute("DELETE FROM export_journal", [])
            .expect("clear rows");
        journal(
            &serde_json::to_string(&["inside.csv"]).expect("relative"),
            4,
        );
        drop(db);
        store(&temp)
            .recover(now, Duration::from_secs(60), 16)
            .expect("recovery after repair");
        assert!(!legitimate.exists());
        assert_eq!(export_journal_count(&temp, export_id), 0);
        // The committed snapshot of this test is unrelated to the planted
        // rows; recovery never touched it.
        assert!(
            store(&temp).load_manifest(Uuid::from_u128(1)).is_ok(),
            "the committed snapshot manifest survives the sweeps"
        );
        let _ = manifest;
    }

    fn store_dir_contents(temp: &TempDir) -> usize {
        std::fs::read_dir(temp.path())
            .expect("store listing")
            .count()
    }

    // -------------------------------------------------------------------------
    // Item 19: injected rename and SQLite failures leave recoverable residue
    // -------------------------------------------------------------------------

    #[cfg(unix)]
    #[test]
    fn injected_rename_failure_fails_typed_and_recovery_cleans_the_residue() {
        use std::os::unix::fs::PermissionsExt;

        let temp = TempDir::new().expect("temp directory");
        let manifest = publish(
            &temp,
            Uuid::from_u128(1),
            Uuid::from_u128(4),
            logical_schema(),
            vec![vec![1]],
        );
        let root = destination_root(&temp);
        let export_id = Uuid::from_u128(400);
        let created_at = at(1_700_100_000);
        let set_dir = artifact_path(&root, &SALES_SET);

        let mut writer = store(&temp)
            .begin_export(
                local_plan(
                    export_id,
                    &manifest,
                    &root,
                    &SALES_SET,
                    ExportFormat::Csv,
                    ExportShape::PartitionedSet,
                ),
                created_at,
            )
            .expect("begin export");
        install_bytes(&mut writer, b"part-zero\n").expect("install part 0");
        assert!(set_dir.is_dir());

        // Sabotage the set directory permissions: the second rename now
        // fails typed because the target directory rejects the write.
        let metadata = std::fs::metadata(&set_dir).expect("set dir metadata");
        let mut permissions = metadata.permissions();
        permissions.set_mode(0o555);
        std::fs::set_permissions(&set_dir, permissions).expect("read-only set directory");
        let staged = writer.create_staged_file().expect("staged part 1");
        let error = writer
            .install_staged_file(staged)
            .expect_err("rename must fail typed");
        assert!(
            matches!(error, StorageError::Io { .. }),
            "the rename failure is a typed I/O error, got {error:?}"
        );
        // The failed writer refuses to commit; the consumed writer already
        // ran its best-effort cleanup on the error path.
        let commit_error = writer
            .commit(provenance(2, created_at))
            .expect_err("failed writer refuses commit");
        assert!(matches!(
            commit_error,
            StorageError::InvalidDraft("export writer cannot commit after a failed install")
        ));

        // Residue: journal rows and the already-installed part-0 file; the
        // staged bytes of the failed part were removed best-effort. No
        // manifest is visible. The destination part-0 file is pre-publication
        // residue: recovery deletes it without a tombstone.
        assert!(store(&temp).load_export_manifest(export_id).is_err());
        assert_eq!(export_publication_count(&temp, export_id), 0);
        assert!(artifact_path(&root, &["reports", "sales", "part-0000000000.csv"]).exists());

        // Restore the directory permissions so the sweep can operate.
        let metadata = std::fs::metadata(&set_dir).expect("set dir metadata");
        let mut permissions = metadata.permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&set_dir, permissions).expect("restore set directory");
        store(&temp)
            .recover(at(1_700_100_500), Duration::from_secs(60), 16)
            .expect("recovery");
        assert!(!artifact_path(&root, &["reports", "sales", "part-0000000000.csv"]).exists());
        assert!(!set_dir.exists(), "the journaled set name is freed");
        assert_eq!(export_journal_count(&temp, export_id), 0);

        // A retry starts from zero and commits end to end.
        let mut retried = store(&temp)
            .begin_export(
                local_plan(
                    export_id,
                    &manifest,
                    &root,
                    &SALES_SET,
                    ExportFormat::Csv,
                    ExportShape::PartitionedSet,
                ),
                at(1_700_100_600),
            )
            .expect("retry after rename failure");
        install_bytes(&mut retried, b"retry-0\n").expect("retry part 0");
        retried
            .commit(provenance(1, at(1_700_100_600)))
            .expect("retry commit");
    }

    #[cfg(unix)]
    #[test]
    fn injected_sqlite_failure_fails_typed_and_retry_starts_from_zero() {
        use std::os::unix::fs::PermissionsExt;

        let temp = TempDir::new().expect("temp directory");
        let manifest = publish(
            &temp,
            Uuid::from_u128(1),
            Uuid::from_u128(4),
            logical_schema(),
            vec![vec![1]],
        );
        let root = destination_root(&temp);
        let export_id = Uuid::from_u128(410);
        let created_at = at(1_700_100_000);
        let database = temp.path().join("metadata.sqlite3");

        let mut writer = store(&temp)
            .begin_export(
                local_plan(
                    export_id,
                    &manifest,
                    &root,
                    &REPORTS,
                    ExportFormat::Csv,
                    ExportShape::SingleFile,
                ),
                created_at,
            )
            .expect("begin export");

        // Make the metadata database unwritable so the durable journal
        // insert fails typed before any rename. Include WAL/SHM sidecars
        // so recovery after restore is not blocked by stale read-only
        // WAL files (SQLite creates them lazily).
        let set_mode = |path: &std::path::Path, mode: u32| {
            if let Ok(meta) = std::fs::metadata(path) {
                let mut perms = meta.permissions();
                perms.set_mode(mode);
                let _ = std::fs::set_permissions(path, perms);
            }
        };
        let wal = temp.path().join("metadata.sqlite3-wal");
        let shm = temp.path().join("metadata.sqlite3-shm");
        let metadata = std::fs::metadata(&database).expect("database metadata");
        let original_mode = metadata.permissions().mode();
        let original_wal_mode = std::fs::metadata(&wal).ok().map(|m| m.permissions().mode());
        let original_shm_mode = std::fs::metadata(&shm).ok().map(|m| m.permissions().mode());
        set_mode(&database, 0o444);
        set_mode(&wal, 0o444);
        set_mode(&shm, 0o444);

        let staged = writer.create_staged_file().expect("staged file");
        let error = writer
            .install_staged_file(staged)
            .expect_err("journal insert must fail typed");
        assert!(
            matches!(error, StorageError::Database(_)),
            "the SQLite failure is typed, got {error:?}"
        );
        let commit_error = writer
            .commit(provenance(1, created_at))
            .expect_err("failed writer refuses commit");
        assert!(matches!(
            commit_error,
            StorageError::InvalidDraft("export writer cannot commit after a failed install")
        ));

        // Restore durability; nothing was renamed, so the only residue is
        // the publication claim, which the writer drop and the sweep clear.
        set_mode(&database, original_mode);
        if let Some(mode) = original_wal_mode {
            set_mode(&wal, mode);
        } else {
            let _ = std::fs::remove_file(&wal);
        }
        if let Some(mode) = original_shm_mode {
            set_mode(&shm, mode);
        } else {
            let _ = std::fs::remove_file(&shm);
        }
        store(&temp)
            .recover(at(1_700_100_500), Duration::from_secs(60), 16)
            .expect("recovery");
        assert_eq!(export_publication_count(&temp, export_id), 0);
        assert_eq!(export_journal_count(&temp, export_id), 0);
        assert!(!staging_dir(&temp, export_id).exists());
        assert!(!artifact_path(&root, &REPORTS).exists());

        // A retry with the same identity starts from zero and commits.
        let mut retried = store(&temp)
            .begin_export(
                local_plan(
                    export_id,
                    &manifest,
                    &root,
                    &REPORTS,
                    ExportFormat::Csv,
                    ExportShape::SingleFile,
                ),
                at(1_700_100_600),
            )
            .expect("retry after SQLite failure");
        install_bytes(&mut retried, b"retry\n").expect("retry install");
        retried
            .commit(provenance(1, at(1_700_100_600)))
            .expect("retry commit");
    }

    // -------------------------------------------------------------------------
    // Item 23: object-store destinations fail closed in v1
    // -------------------------------------------------------------------------

    #[test]
    fn object_store_destinations_fail_typed_before_any_publication_state() {
        let temp = TempDir::new().expect("temp directory");
        let manifest = publish(
            &temp,
            Uuid::from_u128(1),
            Uuid::from_u128(4),
            logical_schema(),
            vec![vec![1]],
        );
        let export_id = Uuid::from_u128(420);
        let plan = ExportPlan::try_new(
            export_id,
            input_identity(&manifest),
            ExportDestination::object_store("s3://bucket/prefix"),
            ExportFormat::Csv,
            ExportPolicy::single_file(),
        );
        assert!(matches!(
            plan,
            Err(StorageError::InvalidConfiguration(
                "export destinations must be managed local roots in v1",
            ))
        ));
        // No publication claim, no staging, no bytes anywhere.
        assert_eq!(export_publication_count(&temp, export_id), 0);
    }

    // -------------------------------------------------------------------------
    // Item 10: single-file and total output byte caps at the install boundary
    // -------------------------------------------------------------------------

    /// Grows the staged file sparsely to `total` bytes: only one real block
    /// is written, so the frozen 2 GiB / 8 GiB ceilings are exercised
    /// without materializing real bytes.
    fn grow_sparsely(staged: &mut StagedExportFile, total: u64) {
        staged.write_bytes(b"x").expect("seed byte");
        staged.flush_buffer().expect("seed flush");
        {
            let file = staged.file().expect("staged file access");
            file.seek(SeekFrom::Start(total - 1)).expect("seek");
            file.write_all(b"y").expect("sparse write");
            assert_eq!(file.metadata().expect("metadata").len(), total);
        }
        staged.refresh_accounting().expect("re-account");
    }

    #[test]
    fn single_file_cap_above_fails_typed_and_writes_no_destination_byte() {
        let temp = TempDir::new().expect("temp directory");
        let manifest = publish(
            &temp,
            Uuid::from_u128(1),
            Uuid::from_u128(4),
            logical_schema(),
            vec![vec![1]],
        );
        let root = destination_root(&temp);
        let created_at = at(1_700_100_000);

        // Above the single-file cap: 2 GiB + 1 byte fails typed and no
        // destination file appears.
        let mut writer = store(&temp)
            .begin_export(
                local_plan(
                    Uuid::from_u128(430),
                    &manifest,
                    &root,
                    &REPORTS,
                    ExportFormat::Csv,
                    ExportShape::SingleFile,
                ),
                created_at,
            )
            .expect("begin oversize export");
        let mut staged = writer.create_staged_file().expect("staged file");
        grow_sparsely(&mut staged, MAX_EXPORT_SINGLE_FILE_BYTES + 1);
        let error = writer
            .install_staged_file(staged)
            .expect_err("single-file cap above must fail typed");
        assert!(matches!(
            error,
            StorageError::ExportLimitExceeded {
                resource: "export single file bytes",
                ..
            }
        ));
        drop(writer);
        assert!(!artifact_path(&root, &REPORTS).exists());
        assert_eq!(export_publication_count(&temp, Uuid::from_u128(430)), 0);
    }

    #[test]
    fn total_output_cap_is_accepted_at_eight_gib_and_enforced_above() {
        let temp = TempDir::new().expect("temp directory");
        let manifest = publish(
            &temp,
            Uuid::from_u128(1),
            Uuid::from_u128(4),
            logical_schema(),
            vec![vec![1]],
        );
        let root = destination_root(&temp);
        let export_id = Uuid::from_u128(431);
        let created_at = at(1_700_100_000);

        // Four parts of exactly 2 GiB each: every install sits exactly on
        // the single-file cap (equal is legal) and the running total lands
        // exactly on the 8 GiB output cap (equal is legal).
        let mut writer = store(&temp)
            .begin_export(
                local_plan(
                    export_id,
                    &manifest,
                    &root,
                    &SALES_SET,
                    ExportFormat::Csv,
                    ExportShape::PartitionedSet,
                ),
                created_at,
            )
            .expect("begin cap-boundary export");
        for sequence in 0..4_u32 {
            let mut staged = writer.create_staged_file().expect("staged part");
            grow_sparsely(&mut staged, MAX_EXPORT_SINGLE_FILE_BYTES);
            writer
                .install_staged_file(staged)
                .expect("part at exactly the caps installs");
            let _ = sequence;
        }
        assert_eq!(
            artifact_path(&root, &SALES_SET)
                .read_dir()
                .expect("set listing")
                .count(),
            4
        );

        // One more byte pushes the artifact total above MAX_EXPORT_OUTPUT_BYTES:
        // typed failure, no fifth file, and the four installed files stay
        // invisible (no manifest commit).
        let mut staged = writer.create_staged_file().expect("fifth staged part");
        staged.write_bytes(b"z").expect("fifth byte");
        staged.flush_buffer().expect("fifth flush");
        let error = writer
            .install_staged_file(staged)
            .expect_err("output total cap must fail typed");
        assert!(matches!(
            error,
            StorageError::ExportLimitExceeded {
                resource: "export output bytes",
                ..
            }
        ));
        let commit_error = writer
            .commit(provenance(1, created_at))
            .expect_err("failed writer refuses commit");
        assert!(matches!(
            commit_error,
            StorageError::InvalidDraft("export writer cannot commit after a failed install")
        ));
        assert!(store(&temp).load_export_manifest(export_id).is_err());

        // Recovery frees the destination set so a retry starts from zero.
        store(&temp)
            .recover(at(1_700_100_500), Duration::from_secs(60), 16)
            .expect("recovery");
        assert!(!artifact_path(&root, &SALES_SET).exists());
        assert_eq!(export_journal_count(&temp, export_id), 0);

        let mut retried = store(&temp)
            .begin_export(
                local_plan(
                    export_id,
                    &manifest,
                    &root,
                    &REPORTS,
                    ExportFormat::Csv,
                    ExportShape::SingleFile,
                ),
                at(1_700_100_600),
            )
            .expect("retry after total-cap failure");
        install_bytes(&mut retried, b"retry\n").expect("retry install");
        retried
            .commit(provenance(1, at(1_700_100_600)))
            .expect("retry commit");
    }
}
