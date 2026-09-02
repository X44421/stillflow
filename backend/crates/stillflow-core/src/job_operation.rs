//! Typed E5-J2 Job operation descriptors.
//!
//! This module deliberately contains only value-level contracts. Durable
//! persistence, execution, and transport projections remain owned by the
//! storage, engine, and API crates respectively.

use std::collections::BTreeSet;
use std::path::PathBuf;

use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use crate::{
    asset_input, ensure_no_secret_fields, snapshot_input, ControlPlaneInput, ExportFormat,
    ExportShape, DATASET_SNAPSHOT_VERSION,
};

/// The closed operation kind set for JobOperation v1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum OperationKind {
    Materialize,
    Verification,
    Profile,
    Export,
}

/// A workspace-bound source asset identity. It contains no credential or raw
/// locator value; the version digest is the stable source binding fact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SourceAssetRef {
    pub workspace_id: Uuid,
    pub source_connection_id: Uuid,
    pub source_asset_id: Uuid,
    #[serde(with = "digest_hex")]
    pub version_digest: [u8; 32],
}

/// A workspace/session-bound immutable Snapshot identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SnapshotRef {
    pub workspace_id: Uuid,
    pub session_id: Uuid,
    pub dataset_id: Uuid,
    pub snapshot_id: Uuid,
    #[serde(with = "digest_hex")]
    pub version_digest: [u8; 32],
    #[serde(with = "digest_hex")]
    pub schema_fingerprint: [u8; 32],
    pub snapshot_version: u16,
}

/// Bounds already admitted by the existing materialization runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MaterializePolicyV1 {
    pub batch_size: usize,
}

/// Bounds and rejection publication policy for E4 verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VerificationPolicyV1 {
    pub batch_size: usize,
    pub publish_rejected_rows: bool,
}

/// Q-R1 request values, kept independent of the Engine implementation type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProfileRequestV1 {
    pub columns: ProfileColumnsV1,
    pub top_k: usize,
    pub histogram_buckets: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub enum ProfileColumnsV1 {
    All,
    Explicit(Vec<String>),
}

/// A transport-neutral destination descriptor for the existing X-R1 export
/// contract. The Engine validates and converts local roots to its domain
/// destination type at dispatch time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub enum ExportDestinationV1 {
    Local {
        root: String,
        components: Vec<String>,
    },
    ObjectStore {
        prefix: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExportRequestV1 {
    pub export_id: Uuid,
    pub format: ExportFormat,
    pub shape: ExportShape,
    pub destination: ExportDestinationV1,
}

/// Variant-specific descriptor carried by JobOperation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub enum OperationDescriptorV1 {
    Materialize {
        source_asset: SourceAssetRef,
        materialize_policy: MaterializePolicyV1,
    },
    Verification {
        snapshot: SnapshotRef,
        verification_policy: VerificationPolicyV1,
    },
    Profile {
        snapshot: SnapshotRef,
        profile_request: ProfileRequestV1,
    },
    Export {
        snapshot: SnapshotRef,
        export_request: ExportRequestV1,
    },
}

/// Durable, closed, versioned operation identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JobOperation {
    pub operation_kind: OperationKind,
    pub operation_version: u16,
    pub descriptor: OperationDescriptorV1,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct JobOperationWire {
    operation_kind: OperationKind,
    operation_version: u16,
    descriptor: OperationDescriptorV1,
}

impl<'de> Deserialize<'de> for JobOperation {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let checked = DuplicateCheckedJson::deserialize(deserializer)?;
        let wire: JobOperationWire =
            serde_json::from_value(checked.0).map_err(de::Error::custom)?;
        Ok(Self {
            operation_kind: wire.operation_kind,
            operation_version: wire.operation_version,
            descriptor: wire.descriptor,
        })
    }
}

/// `serde_json` intentionally accepts duplicate object keys and keeps the
/// last value. JobOperation is part of a digest-bearing identity, so silently
/// reinterpreting duplicate keys would violate the fail-closed canonical JSON
/// contract. This small recursive value visitor rejects duplicates before the
/// typed wire representation is decoded.
struct DuplicateCheckedJson(serde_json::Value);

impl<'de> Deserialize<'de> for DuplicateCheckedJson {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer
            .deserialize_any(DuplicateCheckedJsonVisitor)
            .map(Self)
    }
}

struct DuplicateCheckedJsonVisitor;

impl<'de> Visitor<'de> for DuplicateCheckedJsonVisitor {
    type Value = serde_json::Value;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON value without duplicate object keys")
    }

    fn visit_bool<E: de::Error>(self, value: bool) -> Result<Self::Value, E> {
        Ok(serde_json::Value::Bool(value))
    }

    fn visit_i64<E: de::Error>(self, value: i64) -> Result<Self::Value, E> {
        Ok(serde_json::Value::Number(value.into()))
    }

    fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
        Ok(serde_json::Value::Number(value.into()))
    }

    fn visit_f64<E: de::Error>(self, value: f64) -> Result<Self::Value, E> {
        serde_json::Number::from_f64(value)
            .map(serde_json::Value::Number)
            .ok_or_else(|| E::custom("non-finite JSON number"))
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
        Ok(serde_json::Value::String(value.to_owned()))
    }

    fn visit_string<E: de::Error>(self, value: String) -> Result<Self::Value, E> {
        Ok(serde_json::Value::String(value))
    }

    fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(serde_json::Value::Null)
    }

    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(serde_json::Value::Null)
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        DuplicateCheckedJson::deserialize(deserializer).map(|value| value.0)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut access: A) -> Result<Self::Value, A::Error> {
        let mut values = Vec::new();
        while let Some(value) = access.next_element::<DuplicateCheckedJson>()? {
            values.push(value.0);
        }
        Ok(serde_json::Value::Array(values))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Self::Value, A::Error> {
        let mut values = serde_json::Map::new();
        while let Some(key) = access.next_key::<String>()? {
            if values.contains_key(&key) {
                return Err(de::Error::custom("duplicate JSON object key"));
            }
            let value = access.next_value::<DuplicateCheckedJson>()?.0;
            values.insert(key, value);
        }
        Ok(serde_json::Value::Object(values))
    }
}

impl JobOperation {
    pub const VERSION: u16 = 1;
    pub const DIGEST_DOMAIN: &'static [u8] = b"stillflow.job-operation.descriptor.v1\0";

    pub fn try_new(
        operation_kind: OperationKind,
        descriptor: OperationDescriptorV1,
    ) -> Result<Self, OperationValidationError> {
        let operation = Self {
            operation_kind,
            operation_version: Self::VERSION,
            descriptor,
        };
        operation.validate()?;
        Ok(operation)
    }

    pub fn validate(&self) -> Result<(), OperationValidationError> {
        if self.operation_version != Self::VERSION {
            return Err(OperationValidationError::UnsupportedVersion(
                self.operation_version,
            ));
        }
        let descriptor_kind = match &self.descriptor {
            OperationDescriptorV1::Materialize { .. } => OperationKind::Materialize,
            OperationDescriptorV1::Verification { .. } => OperationKind::Verification,
            OperationDescriptorV1::Profile { .. } => OperationKind::Profile,
            OperationDescriptorV1::Export { .. } => OperationKind::Export,
        };
        if descriptor_kind != self.operation_kind {
            return Err(OperationValidationError::KindMismatch);
        }
        match &self.descriptor {
            OperationDescriptorV1::Materialize {
                source_asset,
                materialize_policy,
            } => {
                validate_source_asset_ref(source_asset)?;
                validate_batch_size(materialize_policy.batch_size)?;
            }
            OperationDescriptorV1::Verification {
                snapshot,
                verification_policy,
            } => {
                validate_snapshot_ref(snapshot)?;
                validate_batch_size(verification_policy.batch_size)?;
            }
            OperationDescriptorV1::Profile {
                snapshot,
                profile_request,
            } => {
                validate_snapshot_ref(snapshot)?;
                validate_profile_request(profile_request)?;
            }
            OperationDescriptorV1::Export {
                snapshot,
                export_request,
            } => {
                validate_snapshot_ref(snapshot)?;
                if export_request.export_id.is_nil() {
                    return Err(OperationValidationError::NilIdentity("export"));
                }
                validate_export_destination(
                    &export_request.destination,
                    export_request.format,
                    export_request.shape,
                )?;
            }
        }
        Ok(())
    }

    /// Canonical, lexicographically ordered descriptor bytes. The operation
    /// kind/version are included so a descriptor cannot be reinterpreted by a
    /// different variant.
    pub fn canonical_descriptor_bytes(&self) -> Result<Vec<u8>, OperationValidationError> {
        self.validate()?;
        let value =
            serde_json::to_value(self).map_err(|_| OperationValidationError::Serialization)?;
        let mut bytes = Vec::new();
        write_canonical_json(&value, &mut bytes)?;
        Ok(bytes)
    }

    pub fn descriptor_digest(&self) -> Result<[u8; 32], OperationValidationError> {
        use sha2::{Digest as _, Sha256};
        let bytes = self.canonical_descriptor_bytes()?;
        let mut digest = Sha256::new();
        digest.update(Self::DIGEST_DOMAIN);
        digest.update(bytes);
        Ok(digest.finalize().into())
    }

    pub fn input(&self) -> ControlPlaneInput {
        match &self.descriptor {
            OperationDescriptorV1::Materialize { source_asset, .. } => {
                asset_input(source_asset.source_asset_id, source_asset.version_digest)
            }
            OperationDescriptorV1::Verification { snapshot, .. }
            | OperationDescriptorV1::Profile { snapshot, .. }
            | OperationDescriptorV1::Export { snapshot, .. } => {
                snapshot_input(snapshot.snapshot_id, snapshot.version_digest)
            }
        }
    }

    pub fn snapshot_ref(&self) -> Option<&SnapshotRef> {
        match &self.descriptor {
            OperationDescriptorV1::Verification { snapshot, .. }
            | OperationDescriptorV1::Profile { snapshot, .. }
            | OperationDescriptorV1::Export { snapshot, .. } => Some(snapshot),
            OperationDescriptorV1::Materialize { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperationValidationError {
    UnsupportedVersion(u16),
    KindMismatch,
    NilIdentity(&'static str),
    ZeroDigest(&'static str),
    InvalidBatchSize,
    InvalidProfileBounds,
    DuplicateProfileColumn,
    InvalidProfileColumn,
    InvalidDestination,
    Serialization,
}

impl std::fmt::Display for OperationValidationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported JobOperation version {version}")
            }
            Self::KindMismatch => {
                formatter.write_str("JobOperation kind does not match descriptor")
            }
            Self::NilIdentity(identity) => {
                write!(formatter, "{identity} identity must not be nil")
            }
            Self::ZeroDigest(identity) => write!(formatter, "{identity} digest must not be zero"),
            Self::InvalidBatchSize => {
                formatter.write_str("operation batch size is outside 1..=65536")
            }
            Self::InvalidProfileBounds => {
                formatter.write_str("profile bounds are outside the authorized range")
            }
            Self::DuplicateProfileColumn => {
                formatter.write_str("profile column selection contains duplicates")
            }
            Self::InvalidProfileColumn => {
                formatter.write_str("profile column selection contains an invalid name")
            }
            Self::InvalidDestination => formatter.write_str("export destination is invalid"),
            Self::Serialization => formatter.write_str("JobOperation canonicalization failed"),
        }
    }
}

impl std::error::Error for OperationValidationError {}

fn validate_source_asset_ref(reference: &SourceAssetRef) -> Result<(), OperationValidationError> {
    for (id, label) in [
        (reference.workspace_id, "workspace"),
        (reference.source_connection_id, "source connection"),
        (reference.source_asset_id, "source asset"),
    ] {
        if id.is_nil() {
            return Err(OperationValidationError::NilIdentity(label));
        }
    }
    if reference.version_digest == [0; 32] {
        return Err(OperationValidationError::ZeroDigest("source asset version"));
    }
    Ok(())
}

fn validate_snapshot_ref(reference: &SnapshotRef) -> Result<(), OperationValidationError> {
    for (id, label) in [
        (reference.workspace_id, "workspace"),
        (reference.session_id, "session"),
        (reference.dataset_id, "dataset"),
        (reference.snapshot_id, "snapshot"),
    ] {
        if id.is_nil() {
            return Err(OperationValidationError::NilIdentity(label));
        }
    }
    if reference.version_digest == [0; 32] {
        return Err(OperationValidationError::ZeroDigest("snapshot version"));
    }
    if reference.schema_fingerprint == [0; 32] {
        return Err(OperationValidationError::ZeroDigest("snapshot schema"));
    }
    if reference.snapshot_version != DATASET_SNAPSHOT_VERSION {
        return Err(OperationValidationError::UnsupportedVersion(
            reference.snapshot_version,
        ));
    }
    Ok(())
}

fn validate_batch_size(batch_size: usize) -> Result<(), OperationValidationError> {
    if !(1..=65_536).contains(&batch_size) {
        return Err(OperationValidationError::InvalidBatchSize);
    }
    Ok(())
}

fn validate_profile_request(request: &ProfileRequestV1) -> Result<(), OperationValidationError> {
    if request.top_k == 0
        || request.top_k > 100
        || request.histogram_buckets == 0
        || request.histogram_buckets > 64
    {
        return Err(OperationValidationError::InvalidProfileBounds);
    }
    if let ProfileColumnsV1::Explicit(columns) = &request.columns {
        if columns.len() > 256 {
            return Err(OperationValidationError::InvalidProfileBounds);
        }
        let mut seen = BTreeSet::new();
        for column in columns {
            if !seen.insert(column) {
                return Err(OperationValidationError::DuplicateProfileColumn);
            }
            if column.is_empty() || column.len() > 256 {
                return Err(OperationValidationError::InvalidProfileColumn);
            }
            ensure_no_secret_fields(&serde_json::Value::String(column.clone()))
                .map_err(|_| OperationValidationError::InvalidProfileColumn)?;
        }
    }
    Ok(())
}

fn validate_export_destination(
    destination: &ExportDestinationV1,
    format: ExportFormat,
    shape: ExportShape,
) -> Result<(), OperationValidationError> {
    match destination {
        ExportDestinationV1::Local { root, components } => {
            if root.is_empty() || !std::path::Path::new(root).is_absolute() || components.is_empty()
            {
                return Err(OperationValidationError::InvalidDestination);
            }
            ensure_no_secret_fields(&serde_json::Value::String(root.clone()))
                .map_err(|_| OperationValidationError::InvalidDestination)?;
            crate::ExportDestination::local(PathBuf::from(root), components.clone(), format, shape)
                .map_err(|_| OperationValidationError::InvalidDestination)?;
        }
        ExportDestinationV1::ObjectStore { prefix } => {
            if prefix.is_empty() {
                return Err(OperationValidationError::InvalidDestination);
            }
            ensure_no_secret_fields(&serde_json::Value::String(prefix.clone()))
                .map_err(|_| OperationValidationError::InvalidDestination)?;
        }
    }
    Ok(())
}

fn write_canonical_json(
    value: &serde_json::Value,
    output: &mut Vec<u8>,
) -> Result<(), OperationValidationError> {
    match value {
        serde_json::Value::Null => output.extend_from_slice(b"null"),
        serde_json::Value::Bool(value) => {
            output.extend_from_slice(if *value { b"true" } else { b"false" })
        }
        serde_json::Value::Number(number) => {
            if !number.is_i64() && !number.is_u64() {
                return Err(OperationValidationError::Serialization);
            }
            output.extend_from_slice(number.to_string().as_bytes());
        }
        serde_json::Value::String(value) => {
            let encoded = serde_json::to_string(value)
                .map_err(|_| OperationValidationError::Serialization)?;
            output.extend_from_slice(encoded.as_bytes());
        }
        serde_json::Value::Array(values) => {
            output.push(b'[');
            for (index, item) in values.iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                write_canonical_json(item, output)?;
            }
            output.push(b']');
        }
        serde_json::Value::Object(values) => {
            output.push(b'{');
            let mut entries = values.iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(right.0));
            for (index, (key, item)) in entries.into_iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                let encoded = serde_json::to_string(key)
                    .map_err(|_| OperationValidationError::Serialization)?;
                output.extend_from_slice(encoded.as_bytes());
                output.push(b':');
                write_canonical_json(item, output)?;
            }
            output.push(b'}');
        }
    }
    Ok(())
}

mod digest_hex {
    use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(value: &[u8; 32], serializer: S) -> Result<S::Ok, S::Error> {
        let mut text = String::with_capacity(64);
        for byte in value {
            text.push(char::from(b"0123456789abcdef"[(byte >> 4) as usize]));
            text.push(char::from(b"0123456789abcdef"[(byte & 0x0f) as usize]));
        }
        text.serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<[u8; 32], D::Error> {
        let text = String::deserialize(deserializer)?;
        if text.len() != 64 {
            return Err(de::Error::custom("digest must be 64 hex characters"));
        }
        let mut value = [0; 32];
        for (target, pair) in value.iter_mut().zip(text.as_bytes().chunks_exact(2)) {
            let high = hex(pair[0]).ok_or_else(|| de::Error::custom("invalid digest"))?;
            let low = hex(pair[1]).ok_or_else(|| de::Error::custom("invalid digest"))?;
            *target = (high << 4) | low;
        }
        Ok(value)
    }

    fn hex(value: u8) -> Option<u8> {
        match value {
            b'0'..=b'9' => Some(value - b'0'),
            b'a'..=b'f' => Some(value - b'a' + 10),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot_ref() -> SnapshotRef {
        SnapshotRef {
            workspace_id: Uuid::from_u128(1),
            session_id: Uuid::from_u128(2),
            dataset_id: Uuid::from_u128(3),
            snapshot_id: Uuid::from_u128(4),
            version_digest: [0x11; 32],
            schema_fingerprint: [0x22; 32],
            snapshot_version: DATASET_SNAPSHOT_VERSION,
        }
    }

    #[test]
    fn descriptor_digest_is_stable_and_roundtrips() {
        let operation = JobOperation::try_new(
            OperationKind::Profile,
            OperationDescriptorV1::Profile {
                snapshot: snapshot_ref(),
                profile_request: ProfileRequestV1 {
                    columns: ProfileColumnsV1::All,
                    top_k: 10,
                    histogram_buckets: 8,
                },
            },
        )
        .expect("valid profile operation");
        let encoded = serde_json::to_value(&operation).expect("serialize operation");
        let decoded: JobOperation = serde_json::from_value(encoded).expect("deserialize operation");
        assert_eq!(decoded, operation);
        assert_eq!(decoded.descriptor_digest(), operation.descriptor_digest());
        assert_eq!(
            operation.canonical_descriptor_bytes(),
            decoded.canonical_descriptor_bytes()
        );
    }

    #[test]
    fn duplicate_descriptor_keys_fail_closed_before_digesting() {
        let operation = JobOperation::try_new(
            OperationKind::Profile,
            OperationDescriptorV1::Profile {
                snapshot: snapshot_ref(),
                profile_request: ProfileRequestV1 {
                    columns: ProfileColumnsV1::All,
                    top_k: 1,
                    histogram_buckets: 1,
                },
            },
        )
        .expect("valid profile operation");
        let encoded = serde_json::to_string(&operation).expect("serialize operation");
        let duplicate = encoded.replacen(
            "\"operationKind\":\"profile\"",
            "\"operationKind\":\"profile\",\"operationKind\":\"profile\"",
            1,
        );
        assert!(serde_json::from_str::<JobOperation>(&duplicate).is_err());
    }

    #[test]
    fn unknown_version_and_kind_mismatch_fail_closed() {
        let descriptor = OperationDescriptorV1::Profile {
            snapshot: snapshot_ref(),
            profile_request: ProfileRequestV1 {
                columns: ProfileColumnsV1::All,
                top_k: 1,
                histogram_buckets: 1,
            },
        };
        let unknown = JobOperation {
            operation_kind: OperationKind::Profile,
            operation_version: 2,
            descriptor: descriptor.clone(),
        };
        assert!(matches!(
            unknown.validate(),
            Err(OperationValidationError::UnsupportedVersion(2))
        ));
        let mismatch = JobOperation {
            operation_kind: OperationKind::Materialize,
            operation_version: JobOperation::VERSION,
            descriptor,
        };
        assert!(matches!(
            mismatch.validate(),
            Err(OperationValidationError::KindMismatch)
        ));
    }

    #[test]
    fn local_export_destination_rejects_secret_like_root() {
        let result = JobOperation::try_new(
            OperationKind::Export,
            OperationDescriptorV1::Export {
                snapshot: snapshot_ref(),
                export_request: ExportRequestV1 {
                    export_id: Uuid::from_u128(5),
                    format: ExportFormat::Jsonl,
                    shape: ExportShape::SingleFile,
                    destination: ExportDestinationV1::Local {
                        root: "/tmp/password=embedded".to_owned(),
                        components: vec!["output".to_owned()],
                    },
                },
            },
        );
        assert!(matches!(
            result,
            Err(OperationValidationError::InvalidDestination)
        ));
    }
}
