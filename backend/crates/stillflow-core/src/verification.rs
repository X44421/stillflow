//! E4 verification artifact provenance contracts.
//!
//! This module implements exactly the approved E4-S1 surfaces frozen in
//! `docs/issues/issue-054-validation-rejected-rows-contract.md` section 7
//! (`InputRef`, `LogicalInputRef`, `SourceRowRef`, `RuleRef`, `ArtifactKind`,
//! `ArtifactSummary`, provenance input/draft/committed types) and sections
//! 8.6/8.7 (fixed reserved `ColumnId` constants). It owns no storage or engine
//! behavior: callers supply [`ArtifactProvenanceInput`], the engine assembles
//! [`ArtifactProvenanceDraft`], and the storage writer computes summary and
//! content digests into the committed [`ArtifactProvenance`].

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// SHA-256 digest bytes used by provenance identities (contract section 7.2).
pub type ContentDigest = [u8; 32];

/// Version of the verification contract implemented by this surface
/// (contract section 11: `VERIFICATION_CONTRACT_VERSION`).
pub const VERIFICATION_CONTRACT_VERSION: u16 = 1;

/// Caller-injected identity of the logical input behind one execution run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "camelCase")]
pub enum InputRef {
    Asset { asset_id: Uuid },
    Snapshot { snapshot_id: Uuid },
}

impl InputRef {
    /// Fixed digest-input tag from contract section 8.1.1 (`0x01` asset,
    /// `0x02` reserved snapshot).
    pub const fn kind_tag(&self) -> u8 {
        match self {
            Self::Asset { .. } => 0x01,
            Self::Snapshot { .. } => 0x02,
        }
    }
}

/// Reference to one authorized logical input version.
///
/// `version_digest` is a caller-injected SHA-256 over the versioned logical
/// input descriptor (asset/snapshot identity plus authorized schema), never
/// over raw rows (contract section 7.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogicalInputRef {
    pub input: InputRef,
    #[serde(with = "digest_hex")]
    pub version_digest: ContentDigest,
}

/// Flattened row identity stored inside artifact rows (contract section 7.3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceRowRef {
    pub input: LogicalInputRef,
    pub source_row_ordinal: u64,
}

/// Auditable rule identity embedded in report rows and rejected-row controls.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuleRef {
    /// SHA-256 of `LogicalPlan::canonical_bytes()`; integrity digest.
    #[serde(with = "digest_hex")]
    pub canonical_plan_digest: ContentDigest,
    /// Existing non-security FNV-1a `PlanFingerprint` bytes; index only.
    #[serde(with = "digest_hex")]
    pub plan_fingerprint: ContentDigest,
    pub node_id: Uuid,
    pub rule_ordinal: u32,
}

/// Kind of one published verification artifact (contract section 7.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ArtifactKind {
    VerificationBundle,
    AcceptedSnapshot,
    ValidationReport,
    RejectedRows,
    DeduplicationReport,
}

impl ArtifactKind {
    /// Fixed digest-input tag from contract section 8.1.1.
    pub const fn tag(self) -> u8 {
        match self {
            Self::VerificationBundle => 0x01,
            Self::AcceptedSnapshot => 0x02,
            Self::ValidationReport => 0x03,
            Self::RejectedRows => 0x04,
            Self::DeduplicationReport => 0x05,
        }
    }

    pub fn try_from_tag(tag: u8) -> Option<Self> {
        match tag {
            0x01 => Some(Self::VerificationBundle),
            0x02 => Some(Self::AcceptedSnapshot),
            0x03 => Some(Self::ValidationReport),
            0x04 => Some(Self::RejectedRows),
            0x05 => Some(Self::DeduplicationReport),
            _ => None,
        }
    }
}

/// Aggregated counters over one complete artifact (contract section 8.1.1).
///
/// `row_count`, `stored_byte_count`, and `partition_count` sum across all
/// sections. `finding_count` counts rows only in finding sections;
/// `warning_count`/`error_count` count severities only in the validation
/// finding section; `duplicate_count` counts rows only in the duplicate
/// finding section. Rule-summary rows are never counted as findings.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactSummary {
    pub row_count: u64,
    pub stored_byte_count: u64,
    pub partition_count: u32,
    pub finding_count: u64,
    pub warning_count: u64,
    pub error_count: u64,
    pub duplicate_count: u64,
}

/// The only structure callers may supply (contract section 7.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactProvenanceInput {
    pub run_id: Uuid,
    pub bundle_id: Uuid,
    pub artifact_id: Uuid,
    pub artifact_kind: ArtifactKind,
    pub session_id: Uuid,
    pub input: LogicalInputRef,
    pub lineage: BTreeSet<Uuid>,
    pub created_at: DateTime<Utc>,
    pub started_at: DateTime<Utc>,
    pub committed_at: DateTime<Utc>,
}

/// Engine-assembled provenance before any storage I/O (contract section 7.2).
///
/// Carries no summary and no content digest: those are writer-computed so
/// callers cannot forge engine-derived integrity fields or build identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactProvenanceDraft {
    pub input: ArtifactProvenanceInput,
    #[serde(with = "digest_hex")]
    pub plan_fingerprint: ContentDigest,
    #[serde(with = "digest_hex")]
    pub canonical_plan_digest: ContentDigest,
    pub engine_contract_version: u16,
    pub engine_build: String,
    pub verification_contract_version: u16,
}

/// Committed provenance with writer-computed summary and content digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactProvenance {
    pub draft: ArtifactProvenanceDraft,
    pub summary: ArtifactSummary,
    #[serde(with = "digest_hex")]
    pub content_digest: ContentDigest,
}

/// Serializes `[u8; 32]` digests as lowercase hex text.
///
/// Persistence rows use compact UTF-8 JSON with the existing storage
/// conventions (contract section 8.1), so fixed byte arrays cross that
/// boundary as hex strings rather than JSON number arrays.
pub(crate) mod digest_hex {
    use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

    const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

    pub fn serialize<S: Serializer>(value: &[u8; 32], serializer: S) -> Result<S::Ok, S::Error> {
        let mut text = String::with_capacity(64);
        for byte in value {
            text.push(HEX_DIGITS[(byte >> 4) as usize] as char);
            text.push(HEX_DIGITS[(byte & 0x0F) as usize] as char);
        }
        text.serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<[u8; 32], D::Error> {
        let text = String::deserialize(deserializer)?;
        let bytes = text.as_bytes();
        if bytes.len() != 64 {
            return Err(de::Error::custom("digest must be 64 hex characters"));
        }
        let mut value = [0_u8; 32];
        for (target, pair) in value.iter_mut().zip(bytes.chunks_exact(2)) {
            let high = hex_value(pair[0]).ok_or_else(|| de::Error::custom("invalid hex digit"))?;
            let low = hex_value(pair[1]).ok_or_else(|| de::Error::custom("invalid hex digit"))?;
            *target = (high << 4) | low;
        }
        Ok(value)
    }

    fn hex_value(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            _ => None,
        }
    }
}

/// Reserved rejected-row control `ColumnId`s (contract section 8.6).
///
/// These are fixed contract constants written once here and never generated
/// at runtime. A source schema containing any of these ids or the matching
/// control names is a preflight `InvalidPlan`.
pub const REJECTED_INPUT_KIND_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0011));
pub const REJECTED_INPUT_ID_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0012));
pub const REJECTED_INPUT_VERSION_DIGEST_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0013));
pub const REJECTED_SOURCE_ROW_ORDINAL_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0014));
pub const REJECTED_KIND_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0015));
pub const REJECTED_PLAN_FINGERPRINT_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0016));
pub const REJECTED_CANONICAL_PLAN_DIGEST_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0017));
pub const REJECTED_NODE_ID_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0018));
pub const REJECTED_RULE_ORDINAL_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0019));

/// Reserved `ColumnId`s of the `ValidationRuleSummary` report section
/// (contract section 8.7).
pub const VALIDATION_RULE_SUMMARY_INPUT_KIND_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0021));
pub const VALIDATION_RULE_SUMMARY_INPUT_ID_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0022));
pub const VALIDATION_RULE_SUMMARY_INPUT_VERSION_DIGEST_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0023));
pub const VALIDATION_RULE_SUMMARY_PLAN_FINGERPRINT_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0024));
pub const VALIDATION_RULE_SUMMARY_CANONICAL_PLAN_DIGEST_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0025));
pub const VALIDATION_RULE_SUMMARY_NODE_ID_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0026));
pub const VALIDATION_RULE_SUMMARY_RULE_ORDINAL_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0027));
pub const VALIDATION_RULE_SUMMARY_MESSAGE_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0028));
pub const VALIDATION_RULE_SUMMARY_EVALUATED_COUNT_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0029));
pub const VALIDATION_RULE_SUMMARY_PASS_COUNT_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_002A));
pub const VALIDATION_RULE_SUMMARY_FAIL_COUNT_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_002B));
pub const VALIDATION_RULE_SUMMARY_WARNING_COUNT_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_002C));
pub const VALIDATION_RULE_SUMMARY_ERROR_COUNT_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_002D));
pub const VALIDATION_RULE_SUMMARY_NULL_COUNT_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_002E));
pub const VALIDATION_RULE_SUMMARY_FALSE_COUNT_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_002F));

/// Reserved `ColumnId`s of the `ValidationFinding` report section
/// (contract section 8.7).
pub const VALIDATION_FINDING_INPUT_KIND_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0031));
pub const VALIDATION_FINDING_INPUT_ID_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0032));
pub const VALIDATION_FINDING_INPUT_VERSION_DIGEST_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0033));
pub const VALIDATION_FINDING_SOURCE_ROW_ORDINAL_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0034));
pub const VALIDATION_FINDING_PLAN_FINGERPRINT_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0035));
pub const VALIDATION_FINDING_CANONICAL_PLAN_DIGEST_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0036));
pub const VALIDATION_FINDING_NODE_ID_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0037));
pub const VALIDATION_FINDING_RULE_ORDINAL_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0038));
pub const VALIDATION_FINDING_SEVERITY_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0039));
pub const VALIDATION_FINDING_PREDICATE_OUTCOME_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_003A));

/// Reserved `ColumnId`s of the `DedupRuleSummary` report section
/// (contract section 8.7).
pub const DEDUP_RULE_SUMMARY_INPUT_KIND_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0041));
pub const DEDUP_RULE_SUMMARY_INPUT_ID_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0042));
pub const DEDUP_RULE_SUMMARY_INPUT_VERSION_DIGEST_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0043));
pub const DEDUP_RULE_SUMMARY_PLAN_FINGERPRINT_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0044));
pub const DEDUP_RULE_SUMMARY_CANONICAL_PLAN_DIGEST_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0045));
pub const DEDUP_RULE_SUMMARY_NODE_ID_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0046));
pub const DEDUP_RULE_SUMMARY_RULE_ORDINAL_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0047));
pub const DEDUP_RULE_SUMMARY_KEY_COLUMN_COUNT_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0048));
pub const DEDUP_RULE_SUMMARY_EVALUATED_COUNT_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0049));
pub const DEDUP_RULE_SUMMARY_UNIQUE_COUNT_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_004A));
pub const DEDUP_RULE_SUMMARY_DUPLICATE_COUNT_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_004B));

/// Reserved `ColumnId`s of the `DuplicateFinding` report section
/// (contract section 8.7).
pub const DUPLICATE_FINDING_INPUT_KIND_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0051));
pub const DUPLICATE_FINDING_INPUT_ID_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0052));
pub const DUPLICATE_FINDING_INPUT_VERSION_DIGEST_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0053));
pub const DUPLICATE_FINDING_SOURCE_ROW_ORDINAL_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0054));
pub const DUPLICATE_FINDING_FIRST_SOURCE_ROW_ORDINAL_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0055));
pub const DUPLICATE_FINDING_PLAN_FINGERPRINT_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0056));
pub const DUPLICATE_FINDING_CANONICAL_PLAN_DIGEST_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0057));
pub const DUPLICATE_FINDING_NODE_ID_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0058));
pub const DUPLICATE_FINDING_RULE_ORDINAL_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0059));
pub const DUPLICATE_FINDING_KEY_COLUMN_COUNT_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_005A));
pub const DUPLICATE_FINDING_ENCODED_KEY_BYTE_COUNT_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_005B));

use crate::logical::ColumnId;

#[cfg(test)]
mod tests {
    use super::*;

    fn fixed_timestamp() -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000, 0).expect("valid timestamp")
    }

    fn reserved_ids() -> Vec<ColumnId> {
        vec![
            REJECTED_INPUT_KIND_COLUMN_ID,
            REJECTED_INPUT_ID_COLUMN_ID,
            REJECTED_INPUT_VERSION_DIGEST_COLUMN_ID,
            REJECTED_SOURCE_ROW_ORDINAL_COLUMN_ID,
            REJECTED_KIND_COLUMN_ID,
            REJECTED_PLAN_FINGERPRINT_COLUMN_ID,
            REJECTED_CANONICAL_PLAN_DIGEST_COLUMN_ID,
            REJECTED_NODE_ID_COLUMN_ID,
            REJECTED_RULE_ORDINAL_COLUMN_ID,
            VALIDATION_RULE_SUMMARY_INPUT_KIND_COLUMN_ID,
            VALIDATION_RULE_SUMMARY_INPUT_ID_COLUMN_ID,
            VALIDATION_RULE_SUMMARY_INPUT_VERSION_DIGEST_COLUMN_ID,
            VALIDATION_RULE_SUMMARY_PLAN_FINGERPRINT_COLUMN_ID,
            VALIDATION_RULE_SUMMARY_CANONICAL_PLAN_DIGEST_COLUMN_ID,
            VALIDATION_RULE_SUMMARY_NODE_ID_COLUMN_ID,
            VALIDATION_RULE_SUMMARY_RULE_ORDINAL_COLUMN_ID,
            VALIDATION_RULE_SUMMARY_MESSAGE_COLUMN_ID,
            VALIDATION_RULE_SUMMARY_EVALUATED_COUNT_COLUMN_ID,
            VALIDATION_RULE_SUMMARY_PASS_COUNT_COLUMN_ID,
            VALIDATION_RULE_SUMMARY_FAIL_COUNT_COLUMN_ID,
            VALIDATION_RULE_SUMMARY_WARNING_COUNT_COLUMN_ID,
            VALIDATION_RULE_SUMMARY_ERROR_COUNT_COLUMN_ID,
            VALIDATION_RULE_SUMMARY_NULL_COUNT_COLUMN_ID,
            VALIDATION_RULE_SUMMARY_FALSE_COUNT_COLUMN_ID,
            VALIDATION_FINDING_INPUT_KIND_COLUMN_ID,
            VALIDATION_FINDING_INPUT_ID_COLUMN_ID,
            VALIDATION_FINDING_INPUT_VERSION_DIGEST_COLUMN_ID,
            VALIDATION_FINDING_SOURCE_ROW_ORDINAL_COLUMN_ID,
            VALIDATION_FINDING_PLAN_FINGERPRINT_COLUMN_ID,
            VALIDATION_FINDING_CANONICAL_PLAN_DIGEST_COLUMN_ID,
            VALIDATION_FINDING_NODE_ID_COLUMN_ID,
            VALIDATION_FINDING_RULE_ORDINAL_COLUMN_ID,
            VALIDATION_FINDING_SEVERITY_COLUMN_ID,
            VALIDATION_FINDING_PREDICATE_OUTCOME_COLUMN_ID,
            DEDUP_RULE_SUMMARY_INPUT_KIND_COLUMN_ID,
            DEDUP_RULE_SUMMARY_INPUT_ID_COLUMN_ID,
            DEDUP_RULE_SUMMARY_INPUT_VERSION_DIGEST_COLUMN_ID,
            DEDUP_RULE_SUMMARY_PLAN_FINGERPRINT_COLUMN_ID,
            DEDUP_RULE_SUMMARY_CANONICAL_PLAN_DIGEST_COLUMN_ID,
            DEDUP_RULE_SUMMARY_NODE_ID_COLUMN_ID,
            DEDUP_RULE_SUMMARY_RULE_ORDINAL_COLUMN_ID,
            DEDUP_RULE_SUMMARY_KEY_COLUMN_COUNT_COLUMN_ID,
            DEDUP_RULE_SUMMARY_EVALUATED_COUNT_COLUMN_ID,
            DEDUP_RULE_SUMMARY_UNIQUE_COUNT_COLUMN_ID,
            DEDUP_RULE_SUMMARY_DUPLICATE_COUNT_COLUMN_ID,
            DUPLICATE_FINDING_INPUT_KIND_COLUMN_ID,
            DUPLICATE_FINDING_INPUT_ID_COLUMN_ID,
            DUPLICATE_FINDING_INPUT_VERSION_DIGEST_COLUMN_ID,
            DUPLICATE_FINDING_SOURCE_ROW_ORDINAL_COLUMN_ID,
            DUPLICATE_FINDING_FIRST_SOURCE_ROW_ORDINAL_COLUMN_ID,
            DUPLICATE_FINDING_PLAN_FINGERPRINT_COLUMN_ID,
            DUPLICATE_FINDING_CANONICAL_PLAN_DIGEST_COLUMN_ID,
            DUPLICATE_FINDING_NODE_ID_COLUMN_ID,
            DUPLICATE_FINDING_RULE_ORDINAL_COLUMN_ID,
            DUPLICATE_FINDING_KEY_COLUMN_COUNT_COLUMN_ID,
            DUPLICATE_FINDING_ENCODED_KEY_BYTE_COUNT_COLUMN_ID,
        ]
    }

    #[test]
    fn reserved_column_ids_are_unique_and_in_the_frozen_namespace() {
        let ids = reserved_ids();
        assert_eq!(ids.len(), 56);
        let mut sorted = ids.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), ids.len(), "reserved ids must be unique");
        for column_id in ids {
            let raw = column_id.as_uuid().as_u128();
            const NAMESPACE: u128 = 0x00E4_C000_0000_0040_0080_0000_0000_0000;
            assert_eq!(
                raw >> 8,
                NAMESPACE,
                "id must stay inside the reserved namespace"
            );
        }
    }

    #[test]
    fn artifact_kind_tags_match_the_frozen_table() {
        assert_eq!(ArtifactKind::VerificationBundle.tag(), 0x01);
        assert_eq!(ArtifactKind::AcceptedSnapshot.tag(), 0x02);
        assert_eq!(ArtifactKind::ValidationReport.tag(), 0x03);
        assert_eq!(ArtifactKind::RejectedRows.tag(), 0x04);
        assert_eq!(ArtifactKind::DeduplicationReport.tag(), 0x05);
        for tag in 0x01_u8..=0x05 {
            let kind = ArtifactKind::try_from_tag(tag).expect("valid tag");
            assert_eq!(kind.tag(), tag);
        }
        assert!(ArtifactKind::try_from_tag(0x00).is_none());
        assert!(ArtifactKind::try_from_tag(0x06).is_none());
    }

    #[test]
    fn provenance_json_roundtrips_with_hex_digests_and_ordered_maps() {
        let draft = ArtifactProvenanceDraft {
            input: ArtifactProvenanceInput {
                run_id: Uuid::from_u128(1),
                bundle_id: Uuid::from_u128(2),
                artifact_id: Uuid::from_u128(3),
                artifact_kind: ArtifactKind::ValidationReport,
                session_id: Uuid::from_u128(4),
                input: LogicalInputRef {
                    input: InputRef::Asset {
                        asset_id: Uuid::from_u128(5),
                    },
                    version_digest: [0xAB; 32],
                },
                lineage: BTreeSet::from([Uuid::from_u128(7), Uuid::from_u128(6)]),
                created_at: fixed_timestamp(),
                started_at: fixed_timestamp(),
                committed_at: fixed_timestamp(),
            },
            plan_fingerprint: [0x01; 32],
            canonical_plan_digest: [0x02; 32],
            engine_contract_version: 1,
            engine_build: "test-build".to_owned(),
            verification_contract_version: VERIFICATION_CONTRACT_VERSION,
        };
        let committed = ArtifactProvenance {
            draft,
            summary: ArtifactSummary::default(),
            content_digest: [0xCD; 32],
        };

        let json = serde_json::to_string(&committed).expect("serialize provenance");
        assert!(!json.contains("[205,"), "digests must be hex, not arrays");
        assert!(json.contains("abababab"));
        let restored: ArtifactProvenance =
            serde_json::from_str(&json).expect("deserialize provenance");
        assert_eq!(restored, committed);
    }
}
