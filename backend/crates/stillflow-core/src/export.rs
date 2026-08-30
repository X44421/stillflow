//! Stable v1 Export domain values frozen by ADR-004 (§§1–8).
//!
//! This module owns the caller-facing export contracts only: the closed
//! format set, artifact shape policy, committed-input identity, managed
//! destination locations, typed result/error-facing values, and the frozen
//! bound and version constants. Manifest persistence lives in
//! `stillflow-storage` and the encoding runtime lives in `stillflow-engine`;
//! no execution or persistence object appears here.

use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{de::Error as DeError, Deserialize, Deserializer, Serialize, Serializer};

use crate::{ConnectorError, ErrorCategory, LogicalSchemaFingerprint, DATASET_SNAPSHOT_VERSION};

/// Maximum total rows across all files of one export artifact (ADR-004 §5).
pub const MAX_EXPORT_ROWS: u64 = 10_000_000;

/// Maximum total finalized bytes across all files of one export artifact (ADR-004 §5).
pub const MAX_EXPORT_OUTPUT_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// Maximum finalized size of one single-file export artifact (ADR-004 §5).
pub const MAX_EXPORT_SINGLE_FILE_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Maximum number of files in one partitioned export artifact set (ADR-004 §5).
pub const MAX_EXPORT_PARTITIONS: u32 = 1_024;

/// Deadline applied when the caller supplies none (ADR-004 §5).
pub const EXPORT_DEFAULT_DEADLINE_SECONDS: u64 = 600;

/// Maximum live staging bytes per store root across concurrent exports (ADR-004 §5).
pub const MAX_EXPORT_TEMP_BYTES: u64 = 16 * 1024 * 1024 * 1024;

/// Concurrent export publications per store root (ADR-004 §5).
pub const MAX_ACTIVE_EXPORT_PUBLISHERS: u16 = 4;

/// Current serialized version of the Export Manifest contract (ADR-004 §7).
pub const EXPORT_MANIFEST_VERSION: u16 = 1;

/// Current version of the frozen format-encoding contract (ADR-004 §3).
pub const EXPORT_FORMAT_CONTRACT_VERSION: u16 = 1;

/// Pinned identifier of the X-R1 export encoders (ADR-004 §3 pinning rule).
///
/// Text encoders are hand-written against the §3 laws; float rendering is
/// pinned per format below. Parquet encoding is pinned by the Apache Arrow 59
/// workspace pin plus this identifier.
pub const EXPORT_ENCODER_VERSION: &str = "stillflow-export-encoder-v1";

/// Pinned JSONL float renderer: `serde_json`'s Ryu shortest round-trip
/// formatting for `f32`/`f64` (ADR-004 §3 and open question 2).
pub const EXPORT_JSONL_FLOAT_ENCODER: &str = "serde_json-ryu-shortest-round-trip-v1";

/// Pinned CSV/TSV float renderer: Rust `std` shortest `Display` for `f32`/`f64`.
pub const EXPORT_TEXT_FLOAT_ENCODER: &str = "rust-std-display-shortest-v1";

/// Maximum path depth below an Allowed Root for destination components (ADR-004 §6).
pub const MAX_EXPORT_PATH_DEPTH: usize = 8;

/// The closed v1 export format set (ADR-004 §3). No other format exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ExportFormat {
    Csv,
    Tsv,
    Jsonl,
    Parquet,
}

impl ExportFormat {
    /// Every format of the frozen v1 set, in contract order.
    pub const ALL: [ExportFormat; 4] = [
        ExportFormat::Csv,
        ExportFormat::Tsv,
        ExportFormat::Jsonl,
        ExportFormat::Parquet,
    ];

    /// The single-file artifact extension of this format.
    pub const fn extension(self) -> &'static str {
        match self {
            ExportFormat::Csv => "csv",
            ExportFormat::Tsv => "tsv",
            ExportFormat::Jsonl => "jsonl",
            ExportFormat::Parquet => "parquet",
        }
    }

    /// The CSV/TSV field delimiter; `None` for JSONL and Parquet.
    pub const fn text_delimiter(self) -> Option<u8> {
        match self {
            ExportFormat::Csv => Some(b','),
            ExportFormat::Tsv => Some(b'\t'),
            ExportFormat::Jsonl | ExportFormat::Parquet => None,
        }
    }

    /// Parses a format name; unknown, future, or non-contract names
    /// (including Instruction/Chat JSONL or Arrow IPC spellings) fail closed.
    pub fn try_from_name(name: &str) -> Result<Self, ExportError> {
        match name {
            "csv" => Ok(ExportFormat::Csv),
            "tsv" => Ok(ExportFormat::Tsv),
            "jsonl" => Ok(ExportFormat::Jsonl),
            "parquet" => Ok(ExportFormat::Parquet),
            _ => Err(ExportError::UnsupportedFormat),
        }
    }
}

/// Artifact shape of one export (ADR-004 §5 partitioning policy).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ExportShape {
    /// One file at the destination path (the default).
    #[default]
    SingleFile,
    /// A `part-<seq:010>.<ext>` set under one destination directory,
    /// zero-based and contiguous, mirroring input partition order.
    PartitionedSet,
}

/// Frozen v1 export policy (ADR-004 §5 partitioning policy).
///
/// The default artifact is a single file; a partitioned set is explicitly
/// requested. The policy is part of the deterministic byte-identity inputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct ExportPolicy {
    pub shape: ExportShape,
}

impl ExportPolicy {
    pub const fn single_file() -> Self {
        Self {
            shape: ExportShape::SingleFile,
        }
    }

    pub const fn partitioned_set() -> Self {
        Self {
            shape: ExportShape::PartitionedSet,
        }
    }
}

/// The full committed-input identity tuple of an export (ADR-004 §2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExportInputIdentity {
    snapshot_id: uuid::Uuid,
    dataset_id: uuid::Uuid,
    session_id: uuid::Uuid,
    source_asset_id: uuid::Uuid,
    schema_fingerprint: LogicalSchemaFingerprint,
    snapshot_version: u16,
}

impl ExportInputIdentity {
    pub fn try_new(
        snapshot_id: uuid::Uuid,
        dataset_id: uuid::Uuid,
        session_id: uuid::Uuid,
        source_asset_id: uuid::Uuid,
        schema_fingerprint: LogicalSchemaFingerprint,
        snapshot_version: u16,
    ) -> Result<Self, ExportError> {
        if snapshot_id.is_nil() {
            return Err(ExportError::NilIdentity("snapshot"));
        }
        if dataset_id.is_nil() {
            return Err(ExportError::NilIdentity("dataset"));
        }
        if session_id.is_nil() {
            return Err(ExportError::NilIdentity("session"));
        }
        if source_asset_id.is_nil() {
            return Err(ExportError::NilIdentity("source asset"));
        }
        if snapshot_version != DATASET_SNAPSHOT_VERSION {
            return Err(ExportError::UnsupportedSnapshotVersion(snapshot_version));
        }
        Ok(Self {
            snapshot_id,
            dataset_id,
            session_id,
            source_asset_id,
            schema_fingerprint,
            snapshot_version,
        })
    }

    pub const fn snapshot_id(&self) -> uuid::Uuid {
        self.snapshot_id
    }

    pub const fn dataset_id(&self) -> uuid::Uuid {
        self.dataset_id
    }

    pub const fn session_id(&self) -> uuid::Uuid {
        self.session_id
    }

    pub const fn source_asset_id(&self) -> uuid::Uuid {
        self.source_asset_id
    }

    pub const fn schema_fingerprint(&self) -> LogicalSchemaFingerprint {
        self.schema_fingerprint
    }

    pub const fn snapshot_version(&self) -> u16 {
        self.snapshot_version
    }
}

/// Validates one destination path component against the ADR-004 §6 grammar
/// `[A-Za-z0-9][A-Za-z0-9._-]{0,127}`.
///
/// Components `.`, `..`, and names beginning with `.` are reserved and
/// rejected. Comparisons are byte-exact and case-sensitive.
pub fn validate_export_component(component: &str) -> Result<(), ExportError> {
    let bytes = component.as_bytes();
    if bytes.is_empty() || bytes.len() > 128 {
        return Err(ExportError::InvalidPathComponent);
    }
    let first = bytes[0];
    if !first.is_ascii_alphanumeric() {
        return Err(ExportError::InvalidPathComponent);
    }
    for &byte in &bytes[1..] {
        if !(byte.is_ascii_alphanumeric() || byte == b'.' || byte == b'_' || byte == b'-') {
            return Err(ExportError::InvalidPathComponent);
        }
    }
    Ok(())
}

/// The managed destination of one export (ADR-004 §1, §6).
///
/// v1 publishes only to local filesystem Allowed Roots; object-store
/// destinations are representable so that requests naming them fail typed
/// instead of silently localizing, but no object-store publication path
/// exists in v1 (ADR-004 §6; E5 wiring is a later dispatch).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExportDestination {
    Local {
        root: PathBuf,
        components: Vec<String>,
    },
    ObjectStore {
        prefix: String,
    },
}

impl ExportDestination {
    /// Builds a local destination below one Allowed Root.
    ///
    /// `root` must be absolute. `components` are the destination path
    /// components below the root: for `ExportShape::SingleFile` the final
    /// component is the artifact file name and must end in exactly the
    /// negotiated format extension; for `ExportShape::PartitionedSet` the
    /// components name the destination directory of the part set. Depth below
    /// the root is at most [`MAX_EXPORT_PATH_DEPTH`]. Component grammar,
    /// depth, traversal, and extension/format mismatch all fail typed here.
    pub fn local(
        root: impl Into<PathBuf>,
        components: Vec<String>,
        format: ExportFormat,
        shape: ExportShape,
    ) -> Result<Self, ExportError> {
        let root = root.into();
        if !root.is_absolute() {
            return Err(ExportError::NonAbsoluteRoot);
        }
        if components.is_empty() {
            return Err(ExportError::EmptyDestinationPath);
        }
        if components.len() > MAX_EXPORT_PATH_DEPTH {
            return Err(ExportError::PathDepthExceeded);
        }
        for component in &components {
            validate_export_component(component)?;
        }
        if shape == ExportShape::SingleFile {
            let name = components.last().ok_or(ExportError::EmptyDestinationPath)?;
            let extension = format!(".{}", format.extension());
            if name.len() <= extension.len() || !name.ends_with(&extension) {
                return Err(ExportError::ExtensionMismatch);
            }
        }
        Ok(Self::Local { root, components })
    }

    /// Builds an object-store destination. The value is representable but
    /// every export attempt fails typed: object-store publication waits for
    /// E5 wiring (ADR-004 §6).
    pub fn object_store(prefix: impl Into<String>) -> Self {
        Self::ObjectStore {
            prefix: prefix.into(),
        }
    }

    /// Returns `true` when this destination is a managed local root.
    pub const fn is_local(&self) -> bool {
        matches!(self, Self::Local { .. })
    }
}

/// Typed Export failure values (ADR-004 §2–§8).
///
/// The taxonomy maps onto the existing stable [`ErrorCategory`] set; no new
/// category is introduced. Messages never embed filesystem paths or cell
/// values (AGENTS rule 10; ADR-004 §7 manifest hygiene).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ExportError {
    NilIdentity(&'static str),
    UnsupportedSnapshotVersion(u16),
    UnsupportedFormat,
    NonAbsoluteRoot,
    EmptyDestinationPath,
    PathDepthExceeded,
    InvalidPathComponent,
    ExtensionMismatch,
    ObjectStoreDestinationUnsupported,
}

impl fmt::Display for ExportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExportError::NilIdentity(identity) => {
                write!(formatter, "{identity} identity must not be nil")
            }
            ExportError::UnsupportedSnapshotVersion(version) => {
                write!(formatter, "unsupported dataset snapshot version {version}")
            }
            ExportError::UnsupportedFormat => {
                write!(
                    formatter,
                    "export format is not part of the frozen v1 format set"
                )
            }
            ExportError::NonAbsoluteRoot => {
                write!(
                    formatter,
                    "export destination root must be an absolute directory"
                )
            }
            ExportError::EmptyDestinationPath => {
                write!(formatter, "export destination path must not be empty")
            }
            ExportError::PathDepthExceeded => {
                write!(
                    formatter,
                    "export destination depth exceeds the frozen limit"
                )
            }
            ExportError::InvalidPathComponent => {
                write!(formatter, "export destination component is invalid")
            }
            ExportError::ExtensionMismatch => {
                write!(
                    formatter,
                    "export destination extension does not match the negotiated format"
                )
            }
            ExportError::ObjectStoreDestinationUnsupported => {
                write!(
                    formatter,
                    "object-store export destinations are not available in v1"
                )
            }
        }
    }
}

impl ExportError {
    /// Maps the typed export failure onto the stable error categories.
    pub const fn category(&self) -> ErrorCategory {
        match self {
            ExportError::NilIdentity(_)
            | ExportError::UnsupportedSnapshotVersion(_)
            | ExportError::UnsupportedFormat
            | ExportError::NonAbsoluteRoot
            | ExportError::EmptyDestinationPath
            | ExportError::PathDepthExceeded
            | ExportError::InvalidPathComponent
            | ExportError::ExtensionMismatch => ErrorCategory::InvalidConfiguration,
            ExportError::ObjectStoreDestinationUnsupported => ErrorCategory::UnsupportedCapability,
        }
    }

    /// Converts the typed export failure into the stable connector error
    /// surface so callers keep one error taxonomy (ADR-004 §8).
    pub fn into_connector_error(self) -> ConnectorError {
        ConnectorError::with_category(
            self.category(),
            false,
            self.to_string(),
            Vec::new(),
            std::collections::BTreeMap::new(),
        )
    }
}

/// One finalized file of a committed export artifact (ADR-004 §1, §7).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportResultFile {
    name: String,
    byte_count: u64,
    digest: String,
}

impl ExportResultFile {
    pub fn try_new(
        name: impl Into<String>,
        byte_count: u64,
        digest: impl Into<String>,
    ) -> Result<Self, ExportError> {
        let digest = digest.into();
        if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(ExportError::UnsupportedFormat);
        }
        Ok(Self {
            name: name.into(),
            byte_count,
            digest,
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

/// Typed result of one committed export (ADR-004 §7, §8).
///
/// This is the caller-facing record of the committed Export Manifest. The
/// deadline-overshoot field honors the ADR-004 §5/§8 overshoot-disclosure law.
/// The set digest is received from the storage manifest plane, the single
/// digest authority, and only shape-checked here.
#[derive(Clone, PartialEq, Eq)]
pub struct ExportResult {
    export_id: uuid::Uuid,
    input: ExportInputIdentity,
    format: ExportFormat,
    shape: ExportShape,
    row_count: u64,
    byte_count: u64,
    files: Vec<ExportResultFile>,
    set_digest: String,
    manifest_version: u16,
    destination_root: PathBuf,
    destination_relative: Vec<String>,
    deadline_overshoot: Option<Duration>,
}

impl ExportResult {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        export_id: uuid::Uuid,
        input: ExportInputIdentity,
        format: ExportFormat,
        shape: ExportShape,
        row_count: u64,
        files: Vec<ExportResultFile>,
        set_digest: impl Into<String>,
        manifest_version: u16,
        destination_root: PathBuf,
        destination_relative: Vec<String>,
        deadline_overshoot: Option<Duration>,
    ) -> Result<Self, ExportError> {
        if export_id.is_nil() {
            return Err(ExportError::NilIdentity("export"));
        }
        if manifest_version != EXPORT_MANIFEST_VERSION {
            return Err(ExportError::UnsupportedSnapshotVersion(manifest_version));
        }
        let set_digest = set_digest.into();
        if set_digest.len() != 64 || !set_digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(ExportError::UnsupportedFormat);
        }
        let mut byte_count = 0_u64;
        for file in &files {
            byte_count = byte_count
                .checked_add(file.byte_count())
                .ok_or(ExportError::UnsupportedFormat)?;
        }
        Ok(Self {
            export_id,
            input,
            format,
            shape,
            row_count,
            byte_count,
            files,
            set_digest,
            manifest_version,
            destination_root,
            destination_relative,
            deadline_overshoot,
        })
    }

    pub const fn export_id(&self) -> uuid::Uuid {
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

    pub const fn row_count(&self) -> u64 {
        self.row_count
    }

    pub const fn byte_count(&self) -> u64 {
        self.byte_count
    }

    pub fn files(&self) -> &[ExportResultFile] {
        &self.files
    }

    pub fn set_digest(&self) -> &str {
        &self.set_digest
    }

    pub const fn manifest_version(&self) -> u16 {
        self.manifest_version
    }

    pub fn destination_root(&self) -> &Path {
        &self.destination_root
    }

    pub fn destination_relative(&self) -> &[String] {
        &self.destination_relative
    }

    pub const fn deadline_overshoot(&self) -> Option<Duration> {
        self.deadline_overshoot
    }
}

impl fmt::Debug for ExportResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExportResult")
            .field("export_id", &self.export_id)
            .field("snapshot_id", &self.input.snapshot_id)
            .field("format", &self.format)
            .field("shape", &self.shape)
            .field("row_count", &self.row_count)
            .field("byte_count", &self.byte_count)
            .field("file_count", &self.files.len())
            .field("set_digest", &self.set_digest)
            .field("manifest_version", &self.manifest_version)
            .field("destination_depth", &self.destination_relative.len())
            .field("deadline_overshoot", &self.deadline_overshoot)
            .finish_non_exhaustive()
    }
}

impl Serialize for ExportInputIdentity {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Data {
            snapshot_id: uuid::Uuid,
            dataset_id: uuid::Uuid,
            session_id: uuid::Uuid,
            source_asset_id: uuid::Uuid,
            schema_fingerprint: LogicalSchemaFingerprint,
            snapshot_version: u16,
        }
        Data {
            snapshot_id: self.snapshot_id,
            dataset_id: self.dataset_id,
            session_id: self.session_id,
            source_asset_id: self.source_asset_id,
            schema_fingerprint: self.schema_fingerprint,
            snapshot_version: self.snapshot_version,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ExportInputIdentity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Data {
            snapshot_id: uuid::Uuid,
            dataset_id: uuid::Uuid,
            session_id: uuid::Uuid,
            source_asset_id: uuid::Uuid,
            schema_fingerprint: LogicalSchemaFingerprint,
            snapshot_version: u16,
        }
        let data = Data::deserialize(deserializer)?;
        Self::try_new(
            data.snapshot_id,
            data.dataset_id,
            data.session_id,
            data.source_asset_id,
            data.schema_fingerprint,
            data.snapshot_version,
        )
        .map_err(DeError::custom)
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use crate::{LogicalSchema, LogicalSchemaFingerprint};

    use super::*;

    fn identity() -> ExportInputIdentity {
        let schema = LogicalSchema::empty();
        ExportInputIdentity::try_new(
            Uuid::from_u128(1),
            Uuid::from_u128(2),
            Uuid::from_u128(3),
            Uuid::from_u128(4),
            LogicalSchemaFingerprint::try_from_schema(&schema).expect("fingerprint"),
            DATASET_SNAPSHOT_VERSION,
        )
        .expect("identity")
    }

    #[test]
    fn bound_constants_match_the_frozen_contract_values() {
        assert_eq!(MAX_EXPORT_ROWS, 10_000_000);
        assert_eq!(MAX_EXPORT_OUTPUT_BYTES, 8_u64 * 1024 * 1024 * 1024);
        assert_eq!(MAX_EXPORT_SINGLE_FILE_BYTES, 2_u64 * 1024 * 1024 * 1024);
        assert_eq!(MAX_EXPORT_PARTITIONS, 1_024);
        assert_eq!(EXPORT_DEFAULT_DEADLINE_SECONDS, 600);
        assert_eq!(MAX_EXPORT_TEMP_BYTES, 16_u64 * 1024 * 1024 * 1024);
        assert_eq!(MAX_ACTIVE_EXPORT_PUBLISHERS, 4);
        assert_eq!(EXPORT_MANIFEST_VERSION, 1);
        assert_eq!(EXPORT_FORMAT_CONTRACT_VERSION, 1);
    }

    #[test]
    fn format_set_is_exactly_the_four_frozen_formats() {
        assert_eq!(
            ExportFormat::ALL,
            [
                ExportFormat::Csv,
                ExportFormat::Tsv,
                ExportFormat::Jsonl,
                ExportFormat::Parquet
            ]
        );
        assert_eq!(ExportFormat::Csv.extension(), "csv");
        assert_eq!(ExportFormat::Tsv.extension(), "tsv");
        assert_eq!(ExportFormat::Jsonl.extension(), "jsonl");
        assert_eq!(ExportFormat::Parquet.extension(), "parquet");
        assert_eq!(ExportFormat::Csv.text_delimiter(), Some(b','));
        assert_eq!(ExportFormat::Tsv.text_delimiter(), Some(b'\t'));
        assert_eq!(ExportFormat::Jsonl.text_delimiter(), None);
    }

    #[test]
    fn unknown_future_and_sanctioned_alternative_names_fail_closed() {
        for name in [
            "instructionJsonl",
            "instruction_jsonl",
            "chatJsonl",
            "chat_jsonl",
            "arrowIpc",
            "arrow_ipc",
            "ipc",
            "Csv",
            "CSV",
            "",
            "xlsx",
        ] {
            assert!(
                matches!(
                    ExportFormat::try_from_name(name),
                    Err(ExportError::UnsupportedFormat)
                ),
                "format name {name:?} must fail closed"
            );
        }
        for (name, expected) in [
            ("csv", ExportFormat::Csv),
            ("tsv", ExportFormat::Tsv),
            ("jsonl", ExportFormat::Jsonl),
            ("parquet", ExportFormat::Parquet),
        ] {
            assert_eq!(ExportFormat::try_from_name(name).expect(name), expected);
        }
    }

    #[test]
    fn input_identity_rejects_nil_and_future_versions() {
        let schema = LogicalSchema::empty();
        let print = LogicalSchemaFingerprint::try_from_schema(&schema).expect("fingerprint");
        assert!(matches!(
            ExportInputIdentity::try_new(
                Uuid::nil(),
                Uuid::from_u128(2),
                Uuid::from_u128(3),
                Uuid::from_u128(4),
                print,
                DATASET_SNAPSHOT_VERSION,
            ),
            Err(ExportError::NilIdentity("snapshot"))
        ));
        assert!(matches!(
            ExportInputIdentity::try_new(
                Uuid::from_u128(1),
                Uuid::nil(),
                Uuid::from_u128(3),
                Uuid::from_u128(4),
                print,
                DATASET_SNAPSHOT_VERSION,
            ),
            Err(ExportError::NilIdentity("dataset"))
        ));
        assert!(matches!(
            ExportInputIdentity::try_new(
                Uuid::from_u128(1),
                Uuid::from_u128(2),
                Uuid::from_u128(3),
                Uuid::nil(),
                print,
                DATASET_SNAPSHOT_VERSION,
            ),
            Err(ExportError::NilIdentity("source asset"))
        ));
        assert!(matches!(
            ExportInputIdentity::try_new(
                Uuid::from_u128(1),
                Uuid::from_u128(2),
                Uuid::from_u128(3),
                Uuid::from_u128(4),
                print,
                DATASET_SNAPSHOT_VERSION + 1,
            ),
            Err(ExportError::UnsupportedSnapshotVersion(2))
        ));
    }

    #[test]
    fn component_grammar_is_total_and_byte_exact() {
        assert!(validate_export_component("data").is_ok());
        assert!(validate_export_component("a").is_ok());
        assert!(validate_export_component("A0._-x").is_ok());
        let max = format!("a{}", "b".repeat(127));
        assert!(validate_export_component(&max).is_ok());
        let too_long = format!("a{}", "b".repeat(128));
        assert!(matches!(
            validate_export_component(&too_long),
            Err(ExportError::InvalidPathComponent)
        ));
        for bad in [
            "", ".hidden", ".", "..", "-lead", "_lead", "sp ace", "sl/ash", "ba\\ck", "~",
        ] {
            assert!(
                matches!(
                    validate_export_component(bad),
                    Err(ExportError::InvalidPathComponent)
                ),
                "component {bad:?} must fail"
            );
        }
    }

    #[test]
    fn local_destination_validates_root_depth_and_extension() {
        let root = "/srv/exports";
        assert!(ExportDestination::local(
            root,
            vec!["data.csv".to_owned()],
            ExportFormat::Csv,
            ExportShape::SingleFile,
        )
        .is_ok());
        assert!(matches!(
            ExportDestination::local(
                root,
                vec!["data.parquet".to_owned()],
                ExportFormat::Csv,
                ExportShape::SingleFile,
            ),
            Err(ExportError::ExtensionMismatch)
        ));
        assert!(matches!(
            ExportDestination::local(
                root,
                vec!["data.txt".to_owned()],
                ExportFormat::Csv,
                ExportShape::SingleFile,
            ),
            Err(ExportError::ExtensionMismatch)
        ));
        assert!(matches!(
            ExportDestination::local(
                root,
                vec![".csv".to_owned()],
                ExportFormat::Csv,
                ExportShape::SingleFile,
            ),
            Err(ExportError::InvalidPathComponent)
        ));
        assert!(matches!(
            ExportDestination::local(
                "relative/root",
                vec!["data.csv".to_owned()],
                ExportFormat::Csv,
                ExportShape::SingleFile,
            ),
            Err(ExportError::NonAbsoluteRoot)
        ));
        assert!(matches!(
            ExportDestination::local(
                root,
                Vec::<String>::new(),
                ExportFormat::Csv,
                ExportShape::SingleFile
            ),
            Err(ExportError::EmptyDestinationPath)
        ));
        assert!(matches!(
            ExportDestination::local(
                root,
                vec!["..".to_owned(), "data.csv".to_owned()],
                ExportFormat::Csv,
                ExportShape::SingleFile,
            ),
            Err(ExportError::InvalidPathComponent)
        ));
        let deep: Vec<String> = (0..9).map(|index| format!("d{index}")).collect();
        assert!(matches!(
            ExportDestination::local(root, deep, ExportFormat::Csv, ExportShape::SingleFile),
            Err(ExportError::PathDepthExceeded)
        ));
        let at_limit: Vec<String> = (0..8)
            .map(|index| {
                if index == 7 {
                    "d7.csv".to_owned()
                } else {
                    format!("d{index}")
                }
            })
            .collect();
        assert!(ExportDestination::local(
            root,
            at_limit,
            ExportFormat::Csv,
            ExportShape::SingleFile,
        )
        .is_ok());
        // Set directories carry no extension requirement.
        assert!(ExportDestination::local(
            root,
            vec!["dataset-a".to_owned()],
            ExportFormat::Csv,
            ExportShape::PartitionedSet,
        )
        .is_ok());
    }

    #[test]
    fn object_store_destination_is_representable_but_flagged() {
        let destination = ExportDestination::object_store("s3://bucket/prefix");
        assert!(!destination.is_local());
        assert_eq!(
            ExportError::ObjectStoreDestinationUnsupported.category(),
            ErrorCategory::UnsupportedCapability
        );
    }

    #[test]
    fn export_result_checks_digest_shape_and_rejects_nil_ids() {
        let files = vec![
            ExportResultFile::try_new("part-0000000000.csv", 10, "aa".repeat(32)).expect("file"),
            ExportResultFile::try_new("part-0000000001.csv", 20, "bb".repeat(32)).expect("file"),
        ];
        let result = ExportResult::try_new(
            Uuid::from_u128(9),
            identity(),
            ExportFormat::Csv,
            ExportShape::PartitionedSet,
            5,
            files,
            "cc".repeat(32),
            EXPORT_MANIFEST_VERSION,
            PathBuf::from("/srv/exports"),
            vec!["dataset-a".to_owned()],
            None,
        )
        .expect("result");
        assert_eq!(result.byte_count(), 30);
        assert_eq!(result.set_digest(), &"cc".repeat(32));
        assert!(matches!(
            ExportResult::try_new(
                Uuid::nil(),
                identity(),
                ExportFormat::Csv,
                ExportShape::SingleFile,
                0,
                Vec::new(),
                "cc".repeat(32),
                EXPORT_MANIFEST_VERSION,
                PathBuf::from("/srv"),
                vec!["data.csv".to_owned()],
                None,
            ),
            Err(ExportError::NilIdentity("export"))
        ));
        assert!(matches!(
            ExportResult::try_new(
                Uuid::from_u128(9),
                identity(),
                ExportFormat::Csv,
                ExportShape::SingleFile,
                0,
                Vec::new(),
                "cc".repeat(31),
                EXPORT_MANIFEST_VERSION,
                PathBuf::from("/srv"),
                vec!["data.csv".to_owned()],
                None,
            ),
            Err(ExportError::UnsupportedFormat)
        ));
        assert!(matches!(
            ExportResult::try_new(
                Uuid::from_u128(9),
                identity(),
                ExportFormat::Csv,
                ExportShape::SingleFile,
                0,
                Vec::new(),
                "cc".repeat(32),
                EXPORT_MANIFEST_VERSION + 1,
                PathBuf::from("/srv"),
                vec!["data.csv".to_owned()],
                None,
            ),
            Err(ExportError::UnsupportedSnapshotVersion(2))
        ));
    }

    #[test]
    fn result_debug_never_embeds_paths() {
        let files = vec![ExportResultFile::try_new("data.csv", 4, "ab".repeat(32)).expect("file")];
        let result = ExportResult::try_new(
            Uuid::from_u128(9),
            identity(),
            ExportFormat::Csv,
            ExportShape::SingleFile,
            1,
            files,
            "cd".repeat(32),
            EXPORT_MANIFEST_VERSION,
            PathBuf::from("/tmp/stillflow-export-test-root"),
            vec!["data.csv".to_owned()],
            Some(Duration::from_millis(12)),
        )
        .expect("result");
        let debug = format!("{result:?}");
        assert!(!debug.contains("/tmp/stillflow-export-test-root"));
        assert!(debug.contains("deadline_overshoot"));
    }

    #[test]
    fn input_identity_serialization_roundtrips() {
        let value = identity();
        let json = serde_json::to_string(&value).expect("serialize");
        let restored: ExportInputIdentity = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(restored, value);
    }

    #[test]
    fn error_categories_stay_inside_the_frozen_taxonomy() {
        assert_eq!(
            ExportError::InvalidPathComponent.category(),
            ErrorCategory::InvalidConfiguration
        );
        assert_eq!(
            ExportError::ObjectStoreDestinationUnsupported.category(),
            ErrorCategory::UnsupportedCapability
        );
        let connector = ExportError::ExtensionMismatch.into_connector_error();
        assert_eq!(connector.category(), ErrorCategory::InvalidConfiguration);
    }
}
