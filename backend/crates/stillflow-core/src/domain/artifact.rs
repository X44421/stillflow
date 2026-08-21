//! Experimental E4 provenance and artifact identities.
//!
//! These types are a contract-shaped probe for Issue #54 / PR #57. They are
//! not an approved public API and must not be merged to `main` from this
//! experimental branch.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Caller-selected logical input identity. Snapshot input is reserved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind", content = "value")]
pub enum InputRef {
    Asset { asset_id: Uuid },
    Snapshot { snapshot_id: Uuid },
}

impl InputRef {
    pub fn id(self) -> Uuid {
        match self {
            Self::Asset { asset_id } => asset_id,
            Self::Snapshot { snapshot_id } => snapshot_id,
        }
    }

    pub fn kind_name(self) -> &'static str {
        match self {
            Self::Asset { .. } => "asset",
            Self::Snapshot { .. } => "snapshot",
        }
    }
}

/// Versioned logical input descriptor. `version_digest` is not raw row bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogicalInputRef {
    pub input: InputRef,
    pub version_digest: [u8; 32],
}

/// Flattened source-row identity stored on report and rejected rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceRowRef {
    pub input: LogicalInputRef,
    pub source_row_ordinal: u64,
}

/// Plan/rule identity stored independently of `stillflow-plan` types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuleRef {
    pub canonical_plan_digest: [u8; 32],
    pub plan_fingerprint: [u8; 32],
    pub node_id: Uuid,
    pub rule_ordinal: u32,
}

/// Published verification artifact kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ArtifactKind {
    VerificationBundle,
    AcceptedSnapshot,
    ValidationReport,
    RejectedRows,
    DeduplicationReport,
}

impl ArtifactKind {
    pub const fn tag(self) -> u8 {
        match self {
            Self::VerificationBundle => 0x01,
            Self::AcceptedSnapshot => 0x02,
            Self::ValidationReport => 0x03,
            Self::RejectedRows => 0x04,
            Self::DeduplicationReport => 0x05,
        }
    }
}

/// Aggregated artifact statistics. Finding counts are section-specific.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
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

impl ArtifactSummary {
    pub fn saturating_add(self, other: Self) -> Self {
        Self {
            row_count: self.row_count.saturating_add(other.row_count),
            stored_byte_count: self
                .stored_byte_count
                .saturating_add(other.stored_byte_count),
            partition_count: self.partition_count.saturating_add(other.partition_count),
            finding_count: self.finding_count.saturating_add(other.finding_count),
            warning_count: self.warning_count.saturating_add(other.warning_count),
            error_count: self.error_count.saturating_add(other.error_count),
            duplicate_count: self.duplicate_count.saturating_add(other.duplicate_count),
        }
    }
}

/// Caller-injected provenance fields. Engine fills plan/build identity.
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

/// Engine-assembled draft. Summary and content digest are writer-computed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactProvenanceDraft {
    pub input: ArtifactProvenanceInput,
    pub plan_fingerprint: [u8; 32],
    pub canonical_plan_digest: [u8; 32],
    pub engine_contract_version: u16,
    pub engine_build: String,
    pub verification_contract_version: u16,
}

/// Committed provenance after storage writes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactProvenance {
    pub draft: ArtifactProvenanceDraft,
    pub summary: ArtifactSummary,
    pub content_digest: [u8; 32],
}

/// Hex-encode 32-byte digests for report Utf8 columns.
pub fn digest_hex(bytes: &[u8; 32]) -> String {
    let mut out = String::with_capacity(64);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}
