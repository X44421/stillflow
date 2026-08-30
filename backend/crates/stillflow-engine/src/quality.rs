//! Q-R2 deterministic findings and QualityReport runtime (ADR-003 §§7–§9;
//! issue #181). Consumes the merged Q-R1 ProfileResult without re-scanning.
//! Persistence, history, API ownership, drift, and Privacy/Leakage detectors
//! remain downstream/out of scope.

use std::collections::BTreeSet;
use std::sync::Arc;

use sha2::{Digest as _, Sha256};
use stillflow_core::{FindingSeverity, LogicalType, RequestContext};
use uuid::Uuid;

use crate::error::{map_context_error, EngineError};
use crate::profile::{
    ColumnProfile, DatasetProfile, ProfileColumnStatus, ProfileResult, ProfileTopValue,
};
use crate::verification::{encode_component, KeyBytes, KeyValue};
use crate::{
    ExecutionEngine, DETECTOR_CONTRACT_VERSION, MAX_OPERATOR_STATE_BYTES, PROFILE_MAX_COLUMNS,
    PROFILE_MAX_HISTOGRAM_BUCKETS, PROFILE_MAX_TOP_K, PROFILING_CONTRACT_VERSION,
    QUALITY_SCORE_VERSION,
};

/// Q-R2 retained-state ceiling. It is a deterministic sub-budget of the
/// existing Engine operator-state allowance, and every retained finding,
/// evidence string/digest, and AI proposal is charged before retention.
pub const QUALITY_STATE_BYTE_BUDGET: usize = MAX_OPERATOR_STATE_BYTES / 2;
pub const QUALITY_MAX_AI_PROPOSALS: usize = PROFILE_MAX_COLUMNS;
pub const QUALITY_MAX_FINDINGS: usize = PROFILE_MAX_COLUMNS * 3 + 16;
pub const QUALITY_MAX_EVIDENCE_REFS_PER_FINDING: usize = 8;
pub const QUALITY_MAX_IDENTITY_BYTES: usize = 256;
pub const QUALITY_MAX_PROVENANCE_REF_BYTES: usize = 1024;
pub const QUALITY_MAX_PLAN_FINGERPRINT_BYTES: usize = 256;

const AI_PROPOSAL_DETECTOR_ID: &str = "ai-proposal";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum FindingCategory {
    Schema,
    Text,
    Duplicate,
    Distribution,
    Privacy,
    Leakage,
}

impl FindingCategory {
    fn as_str(self) -> &'static str {
        match self {
            Self::Schema => "Schema",
            Self::Text => "Text",
            Self::Duplicate => "Duplicate",
            Self::Distribution => "Distribution",
            Self::Privacy => "Privacy",
            Self::Leakage => "Leakage",
        }
    }
}

impl TryFrom<&str> for FindingCategory {
    type Error = EngineError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "Schema" => Ok(Self::Schema),
            "Text" => Ok(Self::Text),
            "Duplicate" => Ok(Self::Duplicate),
            "Distribution" => Ok(Self::Distribution),
            "Privacy" => Ok(Self::Privacy),
            "Leakage" => Ok(Self::Leakage),
            _ => Err(EngineError::InvalidPlan("unknown FindingCategory")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FindingOrigin {
    Deterministic,
    AiProposal,
}

impl FindingOrigin {
    fn as_str(self) -> &'static str {
        match self {
            Self::Deterministic => "Deterministic",
            Self::AiProposal => "AiProposal",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AiIdentity {
    pub model_identity: String,
    pub effect_identity: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FindingProvenance {
    pub run_id: Uuid,
    pub target_reference: String,
    pub resolved_request_digest: String,
    pub policy_version: u16,
    pub scanner_contract_version: u16,
    pub plan_fingerprint: Option<String>,
    pub ai_identity: Option<AiIdentity>,
}

impl FindingProvenance {
    pub fn deterministic(
        run_id: Uuid,
        target_reference: impl Into<String>,
        resolved_request_digest: impl Into<String>,
        plan_fingerprint: Option<String>,
    ) -> Self {
        Self {
            run_id,
            target_reference: target_reference.into(),
            resolved_request_digest: resolved_request_digest.into(),
            policy_version: PROFILING_CONTRACT_VERSION,
            scanner_contract_version: PROFILING_CONTRACT_VERSION,
            plan_fingerprint,
            ai_identity: None,
        }
    }

    fn validate_base(&self) -> Result<(), EngineError> {
        if self.target_reference.is_empty()
            || self.target_reference.len() > QUALITY_MAX_PROVENANCE_REF_BYTES
        {
            return Err(EngineError::BoundExceeded(
                "quality provenance target reference is outside the authorized bound",
            ));
        }
        if self
            .plan_fingerprint
            .as_ref()
            .is_some_and(|value| value.len() > QUALITY_MAX_PLAN_FINGERPRINT_BYTES)
        {
            return Err(EngineError::BoundExceeded(
                "quality provenance plan fingerprint is outside the authorized bound",
            ));
        }
        if !is_lower_sha256(&self.resolved_request_digest) {
            return Err(EngineError::InvalidPlan(
                "quality provenance request digest is invalid",
            ));
        }
        if self.policy_version != PROFILING_CONTRACT_VERSION
            || self.scanner_contract_version != PROFILING_CONTRACT_VERSION
        {
            return Err(EngineError::InvalidPlan(
                "unknown profiling policy/scanner contract version",
            ));
        }
        if self.ai_identity.is_some() {
            return Err(EngineError::InvalidPlan(
                "report provenance must not carry AI identity",
            ));
        }
        Ok(())
    }

    fn for_ai(&self, identity: AiIdentity) -> Self {
        let mut value = self.clone();
        value.ai_identity = Some(identity);
        value
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricEvidence {
    pub metric_path: String,
    pub numerator: Option<i128>,
    pub denominator: Option<u128>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValueDigestEvidence {
    pub column_ref: String,
    pub digests: Vec<String>,
    pub count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowRangeEvidence {
    pub start: u64,
    pub end: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistogramBucketEvidence {
    pub bucket_index: usize,
    pub count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistogramEvidence {
    pub column_ref: String,
    pub buckets: Vec<HistogramBucketEvidence>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FindingEvidence {
    Metric(MetricEvidence),
    ValueDigest(ValueDigestEvidence),
    RowRange(RowRangeEvidence),
    Histogram(HistogramEvidence),
}

#[derive(Debug, Clone)]
pub struct QualityFinding {
    finding_id: String,
    category: FindingCategory,
    severity: FindingSeverity,
    detector_id: &'static str,
    detector_contract_version: u16,
    origin: FindingOrigin,
    message: String,
    evidence_refs: Vec<FindingEvidence>,
    provenance: FindingProvenance,
}

impl QualityFinding {
    pub fn finding_id(&self) -> &str {
        &self.finding_id
    }
    pub fn category(&self) -> FindingCategory {
        self.category
    }
    pub fn severity(&self) -> FindingSeverity {
        self.severity
    }
    pub fn detector_id(&self) -> &str {
        self.detector_id
    }
    pub fn detector_contract_version(&self) -> u16 {
        self.detector_contract_version
    }
    pub fn origin(&self) -> FindingOrigin {
        self.origin
    }
    pub fn message(&self) -> &str {
        &self.message
    }
    pub fn evidence_refs(&self) -> &[FindingEvidence] {
        &self.evidence_refs
    }
    pub fn provenance(&self) -> &FindingProvenance {
        &self.provenance
    }
}

#[derive(Debug, Clone)]
pub struct AiProposalInput {
    finding_id: String,
    category: FindingCategory,
    severity: FindingSeverity,
    message: String,
    evidence_refs: Vec<FindingEvidence>,
    identity: AiIdentity,
}

impl AiProposalInput {
    pub fn new(
        finding_id: impl Into<String>,
        category: FindingCategory,
        severity: FindingSeverity,
        message: impl Into<String>,
        evidence_refs: Vec<FindingEvidence>,
        model_identity: impl Into<String>,
        effect_identity: impl Into<String>,
    ) -> Result<Self, EngineError> {
        let finding_id = finding_id.into();
        let message = message.into();
        let model_identity = model_identity.into();
        let effect_identity = effect_identity.into();
        if finding_id.is_empty() || finding_id.len() > 128 {
            return Err(EngineError::BoundExceeded(
                "AI proposal finding_id is outside the authorized bound",
            ));
        }
        if message.is_empty() || message.len() > crate::PROFILE_MAX_RETAINED_VALUE_BYTES {
            return Err(EngineError::BoundExceeded(
                "AI proposal message is outside the authorized bound",
            ));
        }
        if model_identity.is_empty()
            || effect_identity.is_empty()
            || model_identity.len() > QUALITY_MAX_IDENTITY_BYTES
            || effect_identity.len() > QUALITY_MAX_IDENTITY_BYTES
        {
            return Err(EngineError::BoundExceeded(
                "AI proposal model/effect identity is outside the authorized bound",
            ));
        }
        if evidence_refs.is_empty() || evidence_refs.len() > QUALITY_MAX_EVIDENCE_REFS_PER_FINDING {
            return Err(EngineError::BoundExceeded(
                "AI proposal evidence count is outside the authorized bound",
            ));
        }
        Ok(Self {
            finding_id,
            category,
            severity,
            message,
            evidence_refs,
            identity: AiIdentity {
                model_identity,
                effect_identity,
            },
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QualityScore {
    pub value: Option<u8>,
    pub version: u16,
    pub completeness: bool,
    pub missing_components: Vec<String>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct QualityReport {
    pub artifact_type: &'static str,
    pub artifact_body_version: u16,
    pub profiling_contract_version: u16,
    pub profile_report_digest: String,
    pub findings: Vec<QualityFinding>,
    pub score: QualityScore,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerificationAssociation {
    pub verification_bundle_id: Uuid,
    pub validation_present: bool,
    pub dedup_present: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerificationAssociationState {
    Present(VerificationAssociation),
    Absent,
}

#[derive(Debug, Clone)]
pub struct QualityResult {
    pub run_id: Uuid,
    pub report: QualityReport,
    pub canonical_body: Vec<u8>,
    pub canonical_digest: String,
    pub provenance: FindingProvenance,
    pub verification_association: VerificationAssociationState,
}

#[derive(Debug, Clone)]
pub struct QualityRequest {
    pub profile: ProfileResult,
    pub context: RequestContext,
    pub provenance: FindingProvenance,
    pub verification_association: Option<VerificationAssociation>,
    pub ai_proposals: Vec<AiProposalInput>,
}

impl QualityRequest {
    pub fn new(
        profile: ProfileResult,
        context: RequestContext,
        provenance: FindingProvenance,
    ) -> Result<Self, EngineError> {
        if context.deadline().is_none() {
            return Err(EngineError::InvalidPlan(
                "quality run requires a request deadline",
            ));
        }
        Ok(Self {
            profile,
            context,
            provenance,
            verification_association: None,
            ai_proposals: Vec::new(),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DetectorKind {
    SchemaMaxColumns,
    SchemaNullObservations,
    TextLowUniqueness,
    TextLongValues,
    TextTopConcentration,
    DuplicateRows,
    DistributionDominantBucket,
}

#[derive(Debug, Clone, Copy)]
struct DetectorSpec {
    detector_id: &'static str,
    version: u16,
    category: FindingCategory,
    severity: FindingSeverity,
    kind: DetectorKind,
}

const DETECTOR_IDS_V1: [&str; 7] = [
    "distribution.dominant-bucket",
    "duplicate.rows-present",
    "schema.max-columns",
    "schema.null-observations",
    "text.long-values",
    "text.low-uniqueness",
    "text.top-concentration",
];

const DETECTORS_V1: [DetectorSpec; 7] = [
    DetectorSpec {
        detector_id: "schema.max-columns",
        version: DETECTOR_CONTRACT_VERSION,
        category: FindingCategory::Schema,
        severity: FindingSeverity::Info,
        kind: DetectorKind::SchemaMaxColumns,
    },
    DetectorSpec {
        detector_id: "schema.null-observations",
        version: DETECTOR_CONTRACT_VERSION,
        category: FindingCategory::Schema,
        severity: FindingSeverity::Warning,
        kind: DetectorKind::SchemaNullObservations,
    },
    DetectorSpec {
        detector_id: "text.low-uniqueness",
        version: DETECTOR_CONTRACT_VERSION,
        category: FindingCategory::Text,
        severity: FindingSeverity::Info,
        kind: DetectorKind::TextLowUniqueness,
    },
    DetectorSpec {
        detector_id: "text.long-values",
        version: DETECTOR_CONTRACT_VERSION,
        category: FindingCategory::Text,
        severity: FindingSeverity::Warning,
        kind: DetectorKind::TextLongValues,
    },
    DetectorSpec {
        detector_id: "text.top-concentration",
        version: DETECTOR_CONTRACT_VERSION,
        category: FindingCategory::Text,
        severity: FindingSeverity::Info,
        kind: DetectorKind::TextTopConcentration,
    },
    DetectorSpec {
        detector_id: "duplicate.rows-present",
        version: DETECTOR_CONTRACT_VERSION,
        category: FindingCategory::Duplicate,
        severity: FindingSeverity::Warning,
        kind: DetectorKind::DuplicateRows,
    },
    DetectorSpec {
        detector_id: "distribution.dominant-bucket",
        version: DETECTOR_CONTRACT_VERSION,
        category: FindingCategory::Distribution,
        severity: FindingSeverity::Info,
        kind: DetectorKind::DistributionDominantBucket,
    },
];

fn validate_registry(specs: &[DetectorSpec]) -> Result<(), EngineError> {
    let mut seen = BTreeSet::new();
    for spec in specs {
        if spec.version != DETECTOR_CONTRACT_VERSION {
            return Err(EngineError::InvalidPlan(
                "unknown DETECTOR_CONTRACT_VERSION",
            ));
        }
        if !DETECTOR_IDS_V1.contains(&spec.detector_id) {
            return Err(EngineError::InvalidPlan("unknown detector_id"));
        }
        if !seen.insert(spec.detector_id) {
            return Err(EngineError::InvalidPlan("duplicate detector_id"));
        }
        if matches!(
            spec.category,
            FindingCategory::Privacy | FindingCategory::Leakage
        ) {
            return Err(EngineError::InvalidPlan(
                "Privacy/Leakage v1 detectors are reserved",
            ));
        }
    }
    Ok(())
}

#[derive(Debug)]
struct QualityBudget {
    used: usize,
}

impl QualityBudget {
    fn new() -> Self {
        Self { used: 0 }
    }

    fn charge(&mut self, bytes: usize) -> Result<(), EngineError> {
        self.used = self.used.saturating_add(bytes);
        if self.used > QUALITY_STATE_BYTE_BUDGET {
            return Err(EngineError::BoundExceeded(
                "quality retained state exceeds Engine operator-state budget",
            ));
        }
        Ok(())
    }
}

fn finding_bytes(finding: &QualityFinding) -> usize {
    let mut total = finding.finding_id.len()
        + finding.detector_id.len()
        + finding.message.len()
        + finding.provenance.target_reference.len()
        + finding.provenance.resolved_request_digest.len()
        + finding
            .provenance
            .plan_fingerprint
            .as_ref()
            .map_or(0, String::len);
    for evidence in &finding.evidence_refs {
        total = total.saturating_add(match evidence {
            FindingEvidence::Metric(metric) => metric.metric_path.len() + 48,
            FindingEvidence::ValueDigest(value) => {
                value.column_ref.len() + value.digests.iter().map(String::len).sum::<usize>() + 32
            }
            FindingEvidence::RowRange(_) => 32,
            FindingEvidence::Histogram(histogram) => {
                histogram.column_ref.len() + histogram.buckets.len() * 32
            }
        });
    }
    if let Some(ai) = &finding.provenance.ai_identity {
        total = total
            .saturating_add(ai.model_identity.len())
            .saturating_add(ai.effect_identity.len());
    }
    total
}

fn severity_name(severity: FindingSeverity) -> &'static str {
    match severity {
        FindingSeverity::Info => "Info",
        FindingSeverity::Warning => "Warning",
        FindingSeverity::Error => "Error",
    }
}

fn validate_profile_result(profile: &ProfileResult) -> Result<(), EngineError> {
    if profile.profile.artifact_type != "profile_report"
        || profile.profile.artifact_body_version != 1
        || profile.profile.profiling_contract_version != PROFILING_CONTRACT_VERSION
    {
        return Err(EngineError::InvalidPlan(
            "unknown profile report or profiling contract version",
        ));
    }
    if !is_lower_sha256(&profile.canonical_digest) {
        return Err(EngineError::InvalidPlan("profile digest is invalid"));
    }
    let canonical = profile.profile.canonical_body();
    if canonical != profile.canonical_body {
        return Err(EngineError::InvalidPlan(
            "profile canonical body does not match typed profile",
        ));
    }
    let digest = sha256_hex(&canonical);
    if digest != profile.canonical_digest {
        return Err(EngineError::InvalidPlan(
            "profile canonical digest does not match body",
        ));
    }
    Ok(())
}

fn metric_path_allowed(path: &str) -> bool {
    matches!(
        path,
        "dataset.row_count_scanned"
            | "dataset.column_count_profiled"
            | "dataset.duplicate_row_count"
            | "columns.null_count"
            | "columns.unique_count"
            | "columns.length_stats.long_value_count"
    )
}

fn validate_evidence(
    profile: &DatasetProfile,
    evidence: &FindingEvidence,
) -> Result<(), EngineError> {
    match evidence {
        FindingEvidence::Metric(metric) => {
            if !metric_path_allowed(&metric.metric_path) {
                return Err(EngineError::InvalidPlan("unknown quality metric_path"));
            }
            match (metric.numerator, metric.denominator) {
                (None, None) => {}
                (Some(_), Some(denominator)) if denominator > 0 => {}
                _ => {
                    return Err(EngineError::InvalidPlan(
                        "metric rational evidence is incomplete",
                    ))
                }
            }
        }
        FindingEvidence::ValueDigest(value) => {
            if value.digests.is_empty()
                || value.digests.len() > PROFILE_MAX_TOP_K
                || value.digests.windows(2).any(|pair| pair[0] > pair[1])
                || value.digests.iter().any(|digest| !is_lower_sha256(digest))
            {
                return Err(EngineError::InvalidPlan(
                    "ValueDigestEvidence digest list is invalid",
                ));
            }
            let column = profile
                .columns
                .iter()
                .find(|column| column.name == value.column_ref)
                .ok_or(EngineError::InvalidPlan(
                    "ValueDigestEvidence column_ref is unknown",
                ))?;
            if value.count > column.non_null_count {
                return Err(EngineError::InvalidPlan(
                    "ValueDigestEvidence count exceeds column evidence",
                ));
            }
            let mut available = Vec::new();
            if let Some(top_values) = &column.top_values {
                for top in top_values {
                    available.push(digest_top_value(top)?);
                }
            }
            available.sort();
            if value
                .digests
                .iter()
                .any(|digest| available.binary_search(digest).is_err())
            {
                return Err(EngineError::InvalidPlan(
                    "ValueDigestEvidence is not recomputable from profile",
                ));
            }
        }
        FindingEvidence::RowRange(range) => {
            if range.start > range.end || range.end > profile.dataset.row_count_scanned {
                return Err(EngineError::InvalidPlan(
                    "RowRangeEvidence is outside the scan scope",
                ));
            }
        }
        FindingEvidence::Histogram(histogram) => {
            if histogram.buckets.is_empty()
                || histogram.buckets.len() > PROFILE_MAX_HISTOGRAM_BUCKETS
            {
                return Err(EngineError::InvalidPlan(
                    "HistogramEvidence requires at least one bucket",
                ));
            }
            let column = profile
                .columns
                .iter()
                .find(|column| column.name == histogram.column_ref)
                .ok_or(EngineError::InvalidPlan(
                    "HistogramEvidence column_ref is unknown",
                ))?;
            let actual = column.histogram.as_ref().ok_or(EngineError::InvalidPlan(
                "HistogramEvidence column has no histogram",
            ))?;
            let mut previous = None;
            for bucket in &histogram.buckets {
                if bucket.bucket_index >= actual.counts.len()
                    || actual.counts[bucket.bucket_index] != bucket.count
                {
                    return Err(EngineError::InvalidPlan(
                        "HistogramEvidence is not recomputable from profile",
                    ));
                }
                if previous.is_some_and(|index| index >= bucket.bucket_index) {
                    return Err(EngineError::InvalidPlan(
                        "HistogramEvidence bucket order is invalid",
                    ));
                }
                previous = Some(bucket.bucket_index);
            }
        }
    }
    Ok(())
}

fn digest_top_value(value: &ProfileTopValue) -> Result<String, EngineError> {
    let mut bytes = KeyBytes::new();
    match value {
        ProfileTopValue::Text { value, .. } => {
            encode_component(&LogicalType::Utf8, KeyValue::Utf8(value), &mut bytes)?;
        }
        ProfileTopValue::Bytes { value, .. } => {
            encode_component(&LogicalType::Binary, KeyValue::Binary(value), &mut bytes)?;
        }
    }
    Ok(sha256_hex(bytes.as_slice()))
}

fn rational_pair(numerator: u128, denominator: u128) -> (i128, u128) {
    if numerator == 0 {
        return (0, 1);
    }
    let gcd = gcd_u128(numerator, denominator);
    ((numerator / gcd) as i128, denominator / gcd)
}

fn gcd_u128(a: u128, b: u128) -> u128 {
    if b == 0 {
        a
    } else {
        gcd_u128(b, a % b)
    }
}

fn quality_score(profile: &DatasetProfile) -> QualityScore {
    let rows = profile.dataset.row_count_scanned as u128;
    if rows == 0 {
        return QualityScore {
            value: None,
            version: QUALITY_SCORE_VERSION,
            completeness: false,
            missing_components: Vec::new(),
            reason: Some("no_rows".to_owned()),
        };
    }

    let contributing: Vec<&ColumnProfile> = profile
        .columns
        .iter()
        .filter(|column| column.status == ProfileColumnStatus::Profiled)
        .collect();
    let columns = contributing.len() as u128;
    let total_null: u128 = contributing
        .iter()
        .map(|column| column.null_count as u128)
        .sum();

    let mut missing = Vec::new();
    if columns == 0 {
        missing.push("null".to_owned());
    }
    if profile.dataset.duplicate_row_count.is_none() {
        missing.push("duplicate".to_owned());
    }
    missing.sort();
    let completeness = missing.is_empty();

    let has_null = columns > 0;
    let has_dup = profile.dataset.duplicate_row_count.is_some();
    let has_trunc = profile.dataset.truncated;
    if !has_null && !has_dup && !has_trunc {
        return QualityScore {
            value: None,
            version: QUALITY_SCORE_VERSION,
            completeness: false,
            missing_components: missing,
            reason: Some("missing_penalty_evidence".to_owned()),
        };
    }

    // Common denominator S*C when null evidence exists. When C=0, use S.
    let denominator = if columns > 0 {
        rows.saturating_mul(columns)
    } else {
        rows
    };
    let mut penalty_numerator = 0u128;
    if columns > 0 {
        penalty_numerator = penalty_numerator.saturating_add(40u128.saturating_mul(total_null));
    }
    if let Some(duplicates) = profile.dataset.duplicate_row_count {
        let duplicate_term = if columns > 0 {
            30u128
                .saturating_mul(duplicates as u128)
                .saturating_mul(columns)
        } else {
            30u128.saturating_mul(duplicates as u128)
        };
        penalty_numerator = penalty_numerator.saturating_add(duplicate_term);
    }
    if profile.dataset.truncated {
        penalty_numerator = penalty_numerator.saturating_add(10u128.saturating_mul(denominator));
    }
    let score_numerator = 100u128
        .saturating_mul(denominator)
        .saturating_sub(penalty_numerator);
    let rounded = round_half_even(score_numerator, denominator).min(100) as u8;
    QualityScore {
        value: Some(rounded),
        version: QUALITY_SCORE_VERSION,
        completeness,
        missing_components: missing,
        reason: None,
    }
}

fn round_half_even(numerator: u128, denominator: u128) -> u128 {
    let quotient = numerator / denominator;
    let remainder = numerator % denominator;
    let doubled = remainder.saturating_mul(2);
    if doubled > denominator || (doubled == denominator && quotient % 2 == 1) {
        quotient + 1
    } else {
        quotient
    }
}

fn push_finding(
    findings: &mut Vec<QualityFinding>,
    ids: &mut BTreeSet<String>,
    budget: &mut QualityBudget,
    finding: QualityFinding,
) -> Result<(), EngineError> {
    if findings.len() >= QUALITY_MAX_FINDINGS {
        return Err(EngineError::BoundExceeded(
            "quality finding count exceeds fixed bound",
        ));
    }
    if !ids.insert(finding.finding_id.clone()) {
        return Err(EngineError::InvalidPlan(
            "duplicate finding_id within quality report",
        ));
    }
    for evidence in &finding.evidence_refs {
        validate_evidence_placeholder(evidence)?;
    }
    budget.charge(finding_bytes(&finding))?;
    findings.push(finding);
    Ok(())
}

// Structural validation independent of the profile. Full reproducibility is
// checked immediately before retention by run_detectors / AI proposal routing.
fn validate_evidence_placeholder(evidence: &FindingEvidence) -> Result<(), EngineError> {
    match evidence {
        FindingEvidence::Metric(metric) if metric.metric_path.is_empty() => {
            Err(EngineError::InvalidPlan("empty metric_path"))
        }
        FindingEvidence::ValueDigest(value) if value.column_ref.is_empty() => Err(
            EngineError::InvalidPlan("empty ValueDigestEvidence column_ref"),
        ),
        FindingEvidence::Histogram(value) if value.column_ref.is_empty() => Err(
            EngineError::InvalidPlan("empty HistogramEvidence column_ref"),
        ),
        _ => Ok(()),
    }
}

fn deterministic_finding(
    spec: DetectorSpec,
    finding_id: String,
    message: &'static str,
    evidence_refs: Vec<FindingEvidence>,
    provenance: &FindingProvenance,
) -> QualityFinding {
    QualityFinding {
        finding_id,
        category: spec.category,
        severity: spec.severity,
        detector_id: spec.detector_id,
        detector_contract_version: spec.version,
        origin: FindingOrigin::Deterministic,
        message: message.to_owned(),
        evidence_refs,
        provenance: provenance.clone(),
    }
}

fn run_detectors(
    profile: &DatasetProfile,
    provenance: &FindingProvenance,
    context: &RequestContext,
    budget: &mut QualityBudget,
) -> Result<Vec<QualityFinding>, EngineError> {
    validate_registry(&DETECTORS_V1)?;
    let mut specs = DETECTORS_V1.to_vec();
    specs.sort_by(|left, right| left.detector_id.cmp(right.detector_id));
    let mut findings = Vec::new();
    let mut ids = BTreeSet::new();

    for spec in specs {
        context.ensure_active().map_err(map_context_error)?;
        match spec.kind {
            DetectorKind::SchemaMaxColumns => {
                if profile.dataset.column_count_profiled == PROFILE_MAX_COLUMNS {
                    let evidence = FindingEvidence::Metric(MetricEvidence {
                        metric_path: "dataset.column_count_profiled".to_owned(),
                        numerator: None,
                        denominator: None,
                    });
                    validate_evidence(profile, &evidence)?;
                    push_finding(
                        &mut findings,
                        &mut ids,
                        budget,
                        deterministic_finding(
                            spec,
                            "schema.max-columns".to_owned(),
                            "Profiled schema reached the v1 column ceiling.",
                            vec![evidence],
                            provenance,
                        ),
                    )?;
                }
            }
            DetectorKind::SchemaNullObservations => {
                let contributing: Vec<&ColumnProfile> = profile
                    .columns
                    .iter()
                    .filter(|column| column.status == ProfileColumnStatus::Profiled)
                    .collect();
                let total_null: u128 = contributing
                    .iter()
                    .map(|column| column.null_count as u128)
                    .sum();
                let denominator = (profile.dataset.row_count_scanned as u128)
                    .saturating_mul(contributing.len() as u128);
                if total_null > 0 && denominator > 0 {
                    let (numerator, denominator) = rational_pair(total_null, denominator);
                    let evidence = FindingEvidence::Metric(MetricEvidence {
                        metric_path: "columns.null_count".to_owned(),
                        numerator: Some(numerator),
                        denominator: Some(denominator),
                    });
                    validate_evidence(profile, &evidence)?;
                    push_finding(
                        &mut findings,
                        &mut ids,
                        budget,
                        deterministic_finding(
                            spec,
                            "schema.null-observations".to_owned(),
                            "Null observations are present in the profiled scan scope.",
                            vec![evidence],
                            provenance,
                        ),
                    )?;
                }
            }
            DetectorKind::TextLowUniqueness => {
                for (index, column) in profile.columns.iter().enumerate() {
                    context.ensure_active().map_err(map_context_error)?;
                    if column.logical_type != "utf8" || column.non_null_count <= 1 {
                        continue;
                    }
                    let Some(unique) = column.unique_count else {
                        continue;
                    };
                    if unique.saturating_mul(10) <= column.non_null_count {
                        let (numerator, denominator) =
                            rational_pair(unique as u128, column.non_null_count as u128);
                        let evidence = FindingEvidence::Metric(MetricEvidence {
                            metric_path: "columns.unique_count".to_owned(),
                            numerator: Some(numerator),
                            denominator: Some(denominator),
                        });
                        validate_evidence(profile, &evidence)?;
                        push_finding(
                            &mut findings,
                            &mut ids,
                            budget,
                            deterministic_finding(
                                spec,
                                format!("text.low-uniqueness.{index}"),
                                "A text column has low exact uniqueness within the scan scope.",
                                vec![evidence],
                                provenance,
                            ),
                        )?;
                    }
                }
            }
            DetectorKind::TextLongValues => {
                for (index, column) in profile.columns.iter().enumerate() {
                    context.ensure_active().map_err(map_context_error)?;
                    if column.logical_type != "utf8" {
                        continue;
                    }
                    let Some(length) = &column.length else {
                        continue;
                    };
                    if length.long_value_count == 0 || column.non_null_count == 0 {
                        continue;
                    }
                    let (numerator, denominator) = rational_pair(
                        length.long_value_count as u128,
                        column.non_null_count as u128,
                    );
                    let evidence = FindingEvidence::Metric(MetricEvidence {
                        metric_path: "columns.length_stats.long_value_count".to_owned(),
                        numerator: Some(numerator),
                        denominator: Some(denominator),
                    });
                    validate_evidence(profile, &evidence)?;
                    push_finding(
                        &mut findings,
                        &mut ids,
                        budget,
                        deterministic_finding(
                            spec,
                            format!("text.long-values.{index}"),
                            "A text column contains values above the retained-value boundary.",
                            vec![evidence],
                            provenance,
                        ),
                    )?;
                }
            }
            DetectorKind::TextTopConcentration => {
                for (index, column) in profile.columns.iter().enumerate() {
                    context.ensure_active().map_err(map_context_error)?;
                    if column.logical_type != "utf8" || column.non_null_count == 0 {
                        continue;
                    }
                    let Some(top_values) = &column.top_values else {
                        continue;
                    };
                    let Some(first) = top_values.first() else {
                        continue;
                    };
                    let count = match first {
                        ProfileTopValue::Text { count, .. }
                        | ProfileTopValue::Bytes { count, .. } => *count,
                    };
                    if count.saturating_mul(2) < column.non_null_count {
                        continue;
                    }
                    let digest = digest_top_value(first)?;
                    let evidence = FindingEvidence::ValueDigest(ValueDigestEvidence {
                        column_ref: column.name.clone(),
                        digests: vec![digest],
                        count,
                    });
                    validate_evidence(profile, &evidence)?;
                    push_finding(
                        &mut findings,
                        &mut ids,
                        budget,
                        deterministic_finding(
                            spec,
                            format!("text.top-concentration.{index}"),
                            "A text column is dominated by one retained top value.",
                            vec![evidence],
                            provenance,
                        ),
                    )?;
                }
            }
            DetectorKind::DuplicateRows => {
                if profile.dataset.duplicate_row_count.unwrap_or(0) > 0 {
                    let metric = FindingEvidence::Metric(MetricEvidence {
                        metric_path: "dataset.duplicate_row_count".to_owned(),
                        numerator: None,
                        denominator: None,
                    });
                    let range = FindingEvidence::RowRange(RowRangeEvidence {
                        start: 0,
                        end: profile.dataset.row_count_scanned,
                    });
                    validate_evidence(profile, &metric)?;
                    validate_evidence(profile, &range)?;
                    push_finding(
                        &mut findings,
                        &mut ids,
                        budget,
                        deterministic_finding(
                            spec,
                            "duplicate.rows-present".to_owned(),
                            "Exact duplicate rows are present in the profiled scan scope.",
                            vec![metric, range],
                            provenance,
                        ),
                    )?;
                }
            }
            DetectorKind::DistributionDominantBucket => {
                for (index, column) in profile.columns.iter().enumerate() {
                    context.ensure_active().map_err(map_context_error)?;
                    let Some(histogram) = &column.histogram else {
                        continue;
                    };
                    let total: u64 = histogram.counts.iter().sum();
                    if total == 0 {
                        continue;
                    }
                    let Some((bucket_index, count)) = histogram
                        .counts
                        .iter()
                        .copied()
                        .enumerate()
                        .max_by(|left, right| {
                            left.1.cmp(&right.1).then_with(|| right.0.cmp(&left.0))
                        })
                    else {
                        continue;
                    };
                    if count.saturating_mul(10) < total.saturating_mul(9) {
                        continue;
                    }
                    let evidence = FindingEvidence::Histogram(HistogramEvidence {
                        column_ref: column.name.clone(),
                        buckets: vec![HistogramBucketEvidence {
                            bucket_index,
                            count,
                        }],
                    });
                    validate_evidence(profile, &evidence)?;
                    push_finding(
                        &mut findings,
                        &mut ids,
                        budget,
                        deterministic_finding(
                            spec,
                            format!("distribution.dominant-bucket.{index}"),
                            "A numeric distribution is concentrated in one histogram bucket.",
                            vec![evidence],
                            provenance,
                        ),
                    )?;
                }
            }
        }
    }
    Ok(findings)
}

fn add_ai_proposals(
    profile: &DatasetProfile,
    provenance: &FindingProvenance,
    context: &RequestContext,
    proposals: Vec<AiProposalInput>,
    findings: &mut Vec<QualityFinding>,
    budget: &mut QualityBudget,
) -> Result<(), EngineError> {
    if proposals.len() > QUALITY_MAX_AI_PROPOSALS {
        return Err(EngineError::BoundExceeded(
            "AI proposal count exceeds the fixed Q-R2 bound",
        ));
    }
    let mut ids: BTreeSet<String> = findings
        .iter()
        .map(|finding| finding.finding_id.clone())
        .collect();
    for proposal in proposals {
        context.ensure_active().map_err(map_context_error)?;
        for evidence in &proposal.evidence_refs {
            validate_evidence(profile, evidence)?;
        }
        let finding = QualityFinding {
            finding_id: proposal.finding_id,
            category: proposal.category,
            severity: proposal.severity,
            detector_id: AI_PROPOSAL_DETECTOR_ID,
            detector_contract_version: DETECTOR_CONTRACT_VERSION,
            origin: FindingOrigin::AiProposal,
            message: proposal.message,
            evidence_refs: proposal.evidence_refs,
            provenance: provenance.for_ai(proposal.identity),
        };
        push_finding(findings, &mut ids, budget, finding)?;
    }
    Ok(())
}

impl ExecutionEngine {
    /// Q-R2 deterministic findings + QualityReport. This consumes a completed
    /// Q-R1 ProfileResult and therefore performs no data re-scan.
    pub async fn quality(&self, request: QualityRequest) -> Result<QualityResult, EngineError> {
        request.context.ensure_active().map_err(map_context_error)?;
        let permit = Arc::clone(&self.run_gate)
            .try_acquire_owned()
            .map_err(|_| EngineError::Busy)?;
        let result = self.quality_inner(request).await;
        drop(permit);
        result
    }

    async fn quality_inner(&self, request: QualityRequest) -> Result<QualityResult, EngineError> {
        request.context.ensure_active().map_err(map_context_error)?;
        validate_profile_result(&request.profile)?;
        request.provenance.validate_base()?;
        validate_registry(&DETECTORS_V1)?;

        let mut budget = QualityBudget::new();
        let score = quality_score(&request.profile.profile);
        let mut findings = run_detectors(
            &request.profile.profile,
            &request.provenance,
            &request.context,
            &mut budget,
        )?;
        add_ai_proposals(
            &request.profile.profile,
            &request.provenance,
            &request.context,
            request.ai_proposals,
            &mut findings,
            &mut budget,
        )?;
        findings.sort_by(|left, right| left.finding_id.cmp(&right.finding_id));
        request.context.ensure_active().map_err(map_context_error)?;

        let report = QualityReport {
            artifact_type: "quality_report",
            artifact_body_version: 1,
            profiling_contract_version: PROFILING_CONTRACT_VERSION,
            profile_report_digest: request.profile.canonical_digest.clone(),
            findings,
            score,
        };
        let canonical_body = report.canonical_body();
        let canonical_digest = sha256_hex(&canonical_body);
        let verification_association = match request.verification_association {
            Some(value) => VerificationAssociationState::Present(value),
            None => VerificationAssociationState::Absent,
        };
        Ok(QualityResult {
            run_id: request.provenance.run_id,
            report,
            canonical_body,
            canonical_digest,
            provenance: request.provenance,
            verification_association,
        })
    }
}

#[derive(Debug, Clone)]
enum CVal {
    Null,
    Bool(bool),
    Int(i128),
    Str(String),
    Arr(Vec<CVal>),
    Obj(Vec<(String, CVal)>),
}

impl QualityReport {
    pub fn canonical_body(&self) -> Vec<u8> {
        let findings = self
            .findings
            .iter()
            .map(canonical_finding)
            .collect::<Vec<_>>();
        let mut entries = vec![
            (
                "artifact_body_version".to_owned(),
                CVal::Int(self.artifact_body_version as i128),
            ),
            (
                "artifact_type".to_owned(),
                CVal::Str(self.artifact_type.to_owned()),
            ),
            (
                "completeness".to_owned(),
                CVal::Bool(self.score.completeness),
            ),
            ("findings".to_owned(), CVal::Arr(findings)),
            (
                "missing_components".to_owned(),
                CVal::Arr(
                    self.score
                        .missing_components
                        .iter()
                        .cloned()
                        .map(CVal::Str)
                        .collect(),
                ),
            ),
            (
                "profile_report_digest".to_owned(),
                CVal::Str(self.profile_report_digest.clone()),
            ),
            (
                "profiling_contract_version".to_owned(),
                CVal::Int(self.profiling_contract_version as i128),
            ),
            (
                "quality_score".to_owned(),
                self.score
                    .value
                    .map_or(CVal::Null, |value| CVal::Int(value as i128)),
            ),
            (
                "quality_score_version".to_owned(),
                CVal::Int(self.score.version as i128),
            ),
        ];
        if let Some(reason) = &self.score.reason {
            entries.push(("quality_score_reason".to_owned(), CVal::Str(reason.clone())));
        }
        let mut out = Vec::new();
        write_canonical(&CVal::Obj(entries), &mut out);
        out
    }
}

fn canonical_finding(finding: &QualityFinding) -> CVal {
    CVal::Obj(vec![
        (
            "category".to_owned(),
            CVal::Str(finding.category.as_str().to_owned()),
        ),
        (
            "detector_contract_version".to_owned(),
            CVal::Int(finding.detector_contract_version as i128),
        ),
        (
            "detector_id".to_owned(),
            CVal::Str(finding.detector_id.to_owned()),
        ),
        (
            "evidence_refs".to_owned(),
            CVal::Arr(
                finding
                    .evidence_refs
                    .iter()
                    .map(canonical_evidence)
                    .collect(),
            ),
        ),
        (
            "finding_id".to_owned(),
            CVal::Str(finding.finding_id.clone()),
        ),
        ("message".to_owned(), CVal::Str(finding.message.clone())),
        (
            "origin".to_owned(),
            CVal::Str(finding.origin.as_str().to_owned()),
        ),
        (
            "severity".to_owned(),
            CVal::Str(severity_name(finding.severity).to_owned()),
        ),
    ])
}

fn canonical_evidence(evidence: &FindingEvidence) -> CVal {
    match evidence {
        FindingEvidence::Metric(metric) => {
            let mut entries = vec![
                ("kind".to_owned(), CVal::Str("MetricEvidence".to_owned())),
                (
                    "metric_path".to_owned(),
                    CVal::Str(metric.metric_path.clone()),
                ),
            ];
            if let (Some(numerator), Some(denominator)) = (metric.numerator, metric.denominator) {
                entries.push((
                    "rational".to_owned(),
                    CVal::Obj(vec![
                        ("denominator".to_owned(), CVal::Int(denominator as i128)),
                        ("numerator".to_owned(), CVal::Int(numerator)),
                    ]),
                ));
            }
            CVal::Obj(entries)
        }
        FindingEvidence::ValueDigest(value) => CVal::Obj(vec![
            ("column_ref".to_owned(), CVal::Str(value.column_ref.clone())),
            ("count".to_owned(), CVal::Int(value.count as i128)),
            (
                "digests".to_owned(),
                CVal::Arr(value.digests.iter().cloned().map(CVal::Str).collect()),
            ),
            (
                "kind".to_owned(),
                CVal::Str("ValueDigestEvidence".to_owned()),
            ),
        ]),
        FindingEvidence::RowRange(range) => CVal::Obj(vec![
            ("end".to_owned(), CVal::Int(range.end as i128)),
            ("kind".to_owned(), CVal::Str("RowRangeEvidence".to_owned())),
            ("start".to_owned(), CVal::Int(range.start as i128)),
        ]),
        FindingEvidence::Histogram(histogram) => CVal::Obj(vec![
            (
                "buckets".to_owned(),
                CVal::Arr(
                    histogram
                        .buckets
                        .iter()
                        .map(|bucket| {
                            CVal::Obj(vec![
                                (
                                    "bucket_index".to_owned(),
                                    CVal::Int(bucket.bucket_index as i128),
                                ),
                                ("count".to_owned(), CVal::Int(bucket.count as i128)),
                            ])
                        })
                        .collect(),
                ),
            ),
            (
                "column_ref".to_owned(),
                CVal::Str(histogram.column_ref.clone()),
            ),
            ("kind".to_owned(), CVal::Str("HistogramEvidence".to_owned())),
        ]),
    }
}

fn write_canonical(value: &CVal, out: &mut Vec<u8>) {
    match value {
        CVal::Null => out.extend_from_slice(b"null"),
        CVal::Bool(true) => out.extend_from_slice(b"true"),
        CVal::Bool(false) => out.extend_from_slice(b"false"),
        CVal::Int(value) => out.extend_from_slice(value.to_string().as_bytes()),
        CVal::Str(value) => write_json_string(value, out),
        CVal::Arr(items) => {
            out.push(b'[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                write_canonical(item, out);
            }
            out.push(b']');
        }
        CVal::Obj(entries) => {
            let mut sorted: Vec<&(String, CVal)> = entries.iter().collect();
            sorted.sort_by(|left, right| left.0.cmp(&right.0));
            out.push(b'{');
            for (index, (key, item)) in sorted.iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                write_json_string(key, out);
                out.push(b':');
                write_canonical(item, out);
            }
            out.push(b'}');
        }
    }
}

fn write_json_string(text: &str, out: &mut Vec<u8>) {
    out.push(b'"');
    for character in text.chars() {
        match character {
            '"' => out.extend_from_slice(b"\\\""),
            '\\' => out.extend_from_slice(b"\\\\"),
            '\u{8}' => out.extend_from_slice(b"\\b"),
            '\u{9}' => out.extend_from_slice(b"\\t"),
            '\u{a}' => out.extend_from_slice(b"\\n"),
            '\u{c}' => out.extend_from_slice(b"\\f"),
            '\u{d}' => out.extend_from_slice(b"\\r"),
            control if (control as u32) < 0x20 => {
                out.extend_from_slice(format!("\\u{:04x}", control as u32).as_bytes());
            }
            other => out.extend_from_slice(other.to_string().as_bytes()),
        }
    }
    out.push(b'"');
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex_lower(&hasher.finalize())
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn is_lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use stillflow_connectors::ConnectorRegistry;
    use tokio::time::Instant;

    use crate::profile::{DatasetMetrics, ProfileHistogram, ProfileLengthStats};

    fn context() -> RequestContext {
        RequestContext::with_deadline(Instant::now() + Duration::from_secs(60))
    }

    fn provenance(run_id: Uuid) -> FindingProvenance {
        FindingProvenance::deterministic(
            run_id,
            "asset:test",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            None,
        )
    }

    fn text_column(
        name: &str,
        rows: u64,
        nulls: u64,
        unique: Option<u64>,
        top_values: Option<Vec<ProfileTopValue>>,
        long_values: u64,
    ) -> ColumnProfile {
        ColumnProfile {
            name: name.to_owned(),
            logical_type: "utf8".to_owned(),
            status: ProfileColumnStatus::Profiled,
            null_count: nulls,
            non_null_count: rows.saturating_sub(nulls),
            unique_count: unique,
            distinct_overflow: unique.is_none(),
            empty_count: Some(0),
            min_value: None,
            max_value: None,
            sum: None,
            mean: None,
            sum_overflow: false,
            non_finite_count: None,
            true_count: None,
            false_count: None,
            length: Some(ProfileLengthStats {
                sum_of_lengths: rows as u128,
                min_length: Some(1),
                max_length: Some(if long_values > 0 { 300 } else { 1 }),
                avg_length: None,
                long_value_count: long_values,
                histogram: vec![0; 14],
            }),
            histogram: None,
            top_values,
        }
    }

    fn numeric_column(name: &str, rows: u64, histogram: Vec<u64>) -> ColumnProfile {
        ColumnProfile {
            name: name.to_owned(),
            logical_type: "int64".to_owned(),
            status: ProfileColumnStatus::Profiled,
            null_count: 0,
            non_null_count: rows,
            unique_count: Some(rows),
            distinct_overflow: false,
            empty_count: None,
            min_value: None,
            max_value: None,
            sum: None,
            mean: None,
            sum_overflow: false,
            non_finite_count: None,
            true_count: None,
            false_count: None,
            length: None,
            histogram: Some(ProfileHistogram {
                float_domain: false,
                min: crate::profile::ProfileFloat(0.0),
                max: crate::profile::ProfileFloat(0.0),
                width: crate::profile::ProfileFloat(0.0),
                counts: histogram,
            }),
            top_values: None,
        }
    }

    fn skipped_column(name: &str, rows: u64) -> ColumnProfile {
        ColumnProfile {
            name: name.to_owned(),
            logical_type: "struct".to_owned(),
            status: ProfileColumnStatus::SkippedUnsupportedType,
            null_count: 0,
            non_null_count: rows,
            unique_count: None,
            distinct_overflow: false,
            empty_count: None,
            min_value: None,
            max_value: None,
            sum: None,
            mean: None,
            sum_overflow: false,
            non_finite_count: None,
            true_count: None,
            false_count: None,
            length: None,
            histogram: None,
            top_values: None,
        }
    }

    fn profile_result(
        rows: u64,
        columns: Vec<ColumnProfile>,
        duplicates: Option<u64>,
        truncated: bool,
        scanned_bytes: u64,
    ) -> ProfileResult {
        let profile = DatasetProfile {
            artifact_type: "profile_report",
            artifact_body_version: 1,
            profiling_contract_version: PROFILING_CONTRACT_VERSION,
            dataset: DatasetMetrics {
                row_count_scanned: rows,
                column_count_profiled: columns.len(),
                scanned_bytes,
                truncated,
                distinct_row_count: duplicates.map(|value| rows.saturating_sub(value)),
                duplicate_row_count: duplicates,
                full_row_distinct_overflow: duplicates.is_none(),
            },
            columns,
        };
        let canonical_body = profile.canonical_body();
        let canonical_digest = sha256_hex(&canonical_body);
        ProfileResult {
            run_id: Uuid::from_u128(1),
            profile,
            canonical_body,
            canonical_digest,
        }
    }

    async fn run_quality(
        profile: ProfileResult,
        run_id: Uuid,
    ) -> Result<QualityResult, EngineError> {
        let engine = ExecutionEngine::new(ConnectorRegistry::new());
        let request = QualityRequest::new(profile, context(), provenance(run_id))?;
        engine.quality(request).await
    }

    #[test]
    fn q01_registry_determinism_and_fail_closed_identity_version() {
        validate_registry(&DETECTORS_V1).expect("v1 registry");
        let mut sorted = DETECTORS_V1
            .iter()
            .map(|spec| spec.detector_id)
            .collect::<Vec<_>>();
        sorted.sort();
        assert_eq!(sorted, DETECTOR_IDS_V1);

        let mut duplicate = DETECTORS_V1.to_vec();
        duplicate.push(DETECTORS_V1[0]);
        assert!(matches!(
            validate_registry(&duplicate),
            Err(EngineError::InvalidPlan("duplicate detector_id"))
        ));

        let mut unknown = DETECTORS_V1.to_vec();
        unknown[0].detector_id = "unknown.detector";
        assert!(matches!(
            validate_registry(&unknown),
            Err(EngineError::InvalidPlan("unknown detector_id"))
        ));

        let mut version = DETECTORS_V1.to_vec();
        version[0].version = DETECTOR_CONTRACT_VERSION + 1;
        assert!(matches!(
            validate_registry(&version),
            Err(EngineError::InvalidPlan(
                "unknown DETECTOR_CONTRACT_VERSION"
            ))
        ));
    }

    #[test]
    fn q02_categories_are_exhaustive_and_privacy_leakage_reserved() {
        let all = [
            FindingCategory::Schema,
            FindingCategory::Text,
            FindingCategory::Duplicate,
            FindingCategory::Distribution,
            FindingCategory::Privacy,
            FindingCategory::Leakage,
        ];
        assert_eq!(all.len(), 6);
        assert!(DETECTORS_V1.iter().all(|detector| !matches!(
            detector.category,
            FindingCategory::Privacy | FindingCategory::Leakage
        )));
    }

    #[test]
    fn q03_unknown_category_fails_closed() {
        assert!(FindingCategory::try_from("Schema").is_ok());
        assert!(matches!(
            FindingCategory::try_from("Other"),
            Err(EngineError::InvalidPlan("unknown FindingCategory"))
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn q04_finding_shape_and_severity_mapping_are_exact() {
        let profile = profile_result(
            10,
            vec![text_column("t", 10, 1, Some(2), None, 0)],
            Some(0),
            false,
            100,
        );
        let result = run_quality(profile, Uuid::from_u128(4))
            .await
            .expect("quality");
        let finding = result
            .report
            .findings
            .iter()
            .find(|finding| finding.detector_id() == "schema.null-observations")
            .expect("finding");
        let mut bytes = Vec::new();
        write_canonical(&canonical_finding(finding), &mut bytes);
        let body = String::from_utf8(bytes).expect("utf8");
        for key in [
            "\"category\"",
            "\"detector_contract_version\"",
            "\"detector_id\"",
            "\"evidence_refs\"",
            "\"finding_id\"",
            "\"message\"",
            "\"origin\"",
            "\"severity\"",
        ] {
            assert!(body.contains(key));
        }
        assert!(!body.contains("provenance"));
        assert_eq!(finding.severity(), FindingSeverity::Warning);
    }

    #[test]
    fn q05_all_four_evidence_kinds_are_recomputable_from_report() {
        let top = ProfileTopValue::Text {
            value: "x".to_owned(),
            count: 8,
        };
        let profile = profile_result(
            10,
            vec![
                text_column("t", 10, 0, Some(2), Some(vec![top.clone()]), 0),
                numeric_column("n", 10, vec![9, 1]),
            ],
            Some(2),
            false,
            100,
        );
        let digest = digest_top_value(&top).expect("digest");
        let evidence = [
            FindingEvidence::Metric(MetricEvidence {
                metric_path: "dataset.duplicate_row_count".to_owned(),
                numerator: None,
                denominator: None,
            }),
            FindingEvidence::ValueDigest(ValueDigestEvidence {
                column_ref: "t".to_owned(),
                digests: vec![digest],
                count: 8,
            }),
            FindingEvidence::RowRange(RowRangeEvidence { start: 0, end: 10 }),
            FindingEvidence::Histogram(HistogramEvidence {
                column_ref: "n".to_owned(),
                buckets: vec![HistogramBucketEvidence {
                    bucket_index: 0,
                    count: 9,
                }],
            }),
        ];
        for item in &evidence {
            validate_evidence(&profile.profile, item).expect("recomputable");
        }
    }

    #[test]
    fn q06_value_digest_evidence_contains_digest_not_verbatim_value() {
        let top = ProfileTopValue::Text {
            value: "secret-looking-value".to_owned(),
            count: 5,
        };
        let digest = digest_top_value(&top).expect("digest");
        let evidence = FindingEvidence::ValueDigest(ValueDigestEvidence {
            column_ref: "t".to_owned(),
            digests: vec![digest],
            count: 5,
        });
        let mut bytes = Vec::new();
        write_canonical(&canonical_evidence(&evidence), &mut bytes);
        let body = String::from_utf8(bytes).expect("utf8");
        assert!(!body.contains("secret-looking-value"));
        assert!(body.contains("ValueDigestEvidence"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn q07_provenance_exact_fields_and_ai_identity() {
        let profile = profile_result(
            10,
            vec![text_column(
                "t",
                10,
                0,
                Some(2),
                Some(vec![ProfileTopValue::Text {
                    value: "x".to_owned(),
                    count: 8,
                }]),
                0,
            )],
            Some(0),
            false,
            100,
        );
        let evidence = FindingEvidence::ValueDigest(ValueDigestEvidence {
            column_ref: "t".to_owned(),
            digests: vec![digest_top_value(
                profile.profile.columns[0]
                    .top_values
                    .as_ref()
                    .expect("top")
                    .first()
                    .expect("first"),
            )
            .expect("digest")],
            count: 8,
        });
        let proposal = AiProposalInput::new(
            "ai.1",
            FindingCategory::Text,
            FindingSeverity::Info,
            "AI proposal based on report evidence.",
            vec![evidence],
            "model-v1",
            "effect-v1",
        )
        .expect("proposal");
        let engine = ExecutionEngine::new(ConnectorRegistry::new());
        let mut request = QualityRequest::new(profile, context(), provenance(Uuid::from_u128(7)))
            .expect("request");
        request.ai_proposals.push(proposal);
        let result = engine.quality(request).await.expect("quality");
        let ai = result
            .report
            .findings
            .iter()
            .find(|finding| finding.origin() == FindingOrigin::AiProposal)
            .expect("ai");
        assert_eq!(
            ai.provenance().ai_identity,
            Some(AiIdentity {
                model_identity: "model-v1".to_owned(),
                effect_identity: "effect-v1".to_owned()
            })
        );
        assert!(result.provenance.ai_identity.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn q08_ai_origin_is_structural_and_excluded_from_score() {
        let profile = profile_result(
            10,
            vec![text_column(
                "t",
                10,
                0,
                Some(2),
                Some(vec![ProfileTopValue::Text {
                    value: "x".to_owned(),
                    count: 8,
                }]),
                0,
            )],
            Some(0),
            false,
            100,
        );
        let baseline = run_quality(profile.clone(), Uuid::from_u128(8))
            .await
            .expect("baseline");
        let top = profile.profile.columns[0].top_values.as_ref().expect("top")[0].clone();
        let proposal = AiProposalInput::new(
            "ai.score-independent",
            FindingCategory::Text,
            FindingSeverity::Error,
            "AI proposal remains non-authoritative.",
            vec![FindingEvidence::ValueDigest(ValueDigestEvidence {
                column_ref: "t".to_owned(),
                digests: vec![digest_top_value(&top).expect("digest")],
                count: 8,
            })],
            "model",
            "effect",
        )
        .expect("proposal");
        let engine = ExecutionEngine::new(ConnectorRegistry::new());
        let mut request = QualityRequest::new(profile, context(), provenance(Uuid::from_u128(9)))
            .expect("request");
        request.ai_proposals.push(proposal);
        let with_ai = engine.quality(request).await.expect("quality");
        assert_eq!(baseline.report.score, with_ai.report.score);
        let ai = with_ai
            .report
            .findings
            .iter()
            .find(|finding| finding.finding_id() == "ai.score-independent")
            .expect("ai");
        assert_eq!(ai.detector_id(), AI_PROPOSAL_DETECTOR_ID);
        assert_eq!(ai.origin(), FindingOrigin::AiProposal);
    }

    #[test]
    fn q09_quality_score_normative_vectors_v1_v2() {
        let v1_columns = (0..10)
            .map(|index| text_column(&format!("c{index}"), 1000, 50, Some(100), None, 0))
            .collect();
        let v1 = profile_result(1000, v1_columns, Some(100), false, 1000);
        assert_eq!(quality_score(&v1.profile).value, Some(95));

        let nulls = [7, 7, 6, 6, 6, 6, 6, 6];
        let v2_columns = nulls
            .iter()
            .enumerate()
            .map(|(index, nulls)| {
                text_column(&format!("c{index}"), 200, *nulls, Some(100), None, 0)
            })
            .collect();
        let v2 = profile_result(200, v2_columns, Some(15), false, 1000);
        assert_eq!(quality_score(&v2.profile).value, Some(96));
    }

    #[test]
    fn q10_missing_evidence_and_truncation_semantics() {
        let empty = profile_result(
            0,
            vec![text_column("t", 0, 0, Some(0), None, 0)],
            Some(0),
            false,
            0,
        );
        let score = quality_score(&empty.profile);
        assert_eq!(score.value, None);
        assert_eq!(score.reason.as_deref(), Some("no_rows"));

        let missing_dup = profile_result(
            10,
            vec![text_column("t", 10, 0, Some(5), None, 0)],
            None,
            false,
            10,
        );
        let score = quality_score(&missing_dup.profile);
        assert_eq!(score.value, Some(100));
        assert!(!score.completeness);
        assert_eq!(score.missing_components, vec!["duplicate"]);

        let missing_all = profile_result(10, vec![skipped_column("s", 10)], None, false, 10);
        let score = quality_score(&missing_all.profile);
        assert_eq!(score.value, None);
        assert_eq!(score.reason.as_deref(), Some("missing_penalty_evidence"));

        let truncated = profile_result(
            10,
            vec![text_column("t", 10, 0, Some(10), None, 0)],
            Some(0),
            true,
            10,
        );
        assert_eq!(quality_score(&truncated.profile).value, Some(90));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn q11_quality_report_canonical_golden_and_sha256() {
        let profile = profile_result(
            1,
            vec![text_column("t", 1, 0, Some(1), None, 0)],
            Some(0),
            false,
            1,
        );
        let result = run_quality(profile, Uuid::from_u128(11))
            .await
            .expect("quality");
        let body = String::from_utf8(result.canonical_body.clone()).expect("utf8");
        let expected = "{\"artifact_body_version\":1,\"artifact_type\":\"quality_report\",\"completeness\":true,\"findings\":[],\"missing_components\":[],\"profile_report_digest\":\"abe48782c77348eaadb6a09e72b27050dc6af6ba11a595feb5bdb9688e564ec4\",\"profiling_contract_version\":1,\"quality_score\":100,\"quality_score_version\":1}";
        assert_eq!(
            result.report.profile_report_digest,
            "abe48782c77348eaadb6a09e72b27050dc6af6ba11a595feb5bdb9688e564ec4"
        );
        assert_eq!(body, expected);
        assert_eq!(
            result.canonical_digest,
            "72aaea8275a857ce86e2cabe128f5e0849de817df1c164479cd65693b7bbf319"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn q12_run_id_is_excluded_from_canonical_body_and_digest() {
        let profile = profile_result(
            1,
            vec![text_column("t", 1, 0, Some(1), None, 0)],
            Some(0),
            false,
            1,
        );
        let left = run_quality(profile.clone(), Uuid::from_u128(12))
            .await
            .expect("left");
        let right = run_quality(profile, Uuid::from_u128(13))
            .await
            .expect("right");
        assert_ne!(left.run_id, right.run_id);
        assert_eq!(left.canonical_body, right.canonical_body);
        assert_eq!(left.canonical_digest, right.canonical_digest);
        let body = String::from_utf8(left.canonical_body).expect("utf8");
        assert!(!body.contains("run_id"));
        assert!(!body.contains("wall_clock"));
        assert!(!body.contains("timestamp"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn q13_partition_packaging_does_not_change_quality_output() {
        let columns = vec![numeric_column("n", 10, vec![9, 1])];
        let left = profile_result(10, columns.clone(), Some(1), false, 100);
        let right = profile_result(10, columns, Some(1), false, 500);
        assert_eq!(left.canonical_digest, right.canonical_digest);
        let left = run_quality(left, Uuid::from_u128(13)).await.expect("left");
        let right = run_quality(right, Uuid::from_u128(13))
            .await
            .expect("right");
        assert_eq!(left.canonical_body, right.canonical_body);
        assert_eq!(left.canonical_digest, right.canonical_digest);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn q14_verification_association_present_or_explicitly_absent() {
        let profile = profile_result(
            1,
            vec![text_column("t", 1, 0, Some(1), None, 0)],
            Some(0),
            false,
            1,
        );
        let absent = run_quality(profile.clone(), Uuid::from_u128(14))
            .await
            .expect("absent");
        assert_eq!(
            absent.verification_association,
            VerificationAssociationState::Absent
        );

        let engine = ExecutionEngine::new(ConnectorRegistry::new());
        let mut request = QualityRequest::new(profile, context(), provenance(Uuid::from_u128(14)))
            .expect("request");
        let association = VerificationAssociation {
            verification_bundle_id: Uuid::from_u128(99),
            validation_present: true,
            dedup_present: true,
        };
        request.verification_association = Some(association.clone());
        let present = engine.quality(request).await.expect("present");
        assert_eq!(
            present.verification_association,
            VerificationAssociationState::Present(association)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn q15_high_cardinality_profile_is_bounded_and_budget_overflow_fails_typed() {
        let columns = (0..PROFILE_MAX_COLUMNS)
            .map(|index| {
                let mut column = text_column(&format!("c{index}"), 100_001, 0, None, None, 0);
                column.distinct_overflow = true;
                column
            })
            .collect();
        let profile = profile_result(100_001, columns, None, false, 1000);
        let result = run_quality(profile, Uuid::from_u128(15))
            .await
            .expect("bounded");
        assert!(result.report.findings.len() <= QUALITY_MAX_FINDINGS);

        let mut budget = QualityBudget::new();
        assert!(matches!(
            budget.charge(QUALITY_STATE_BYTE_BUDGET + 1),
            Err(EngineError::BoundExceeded(
                "quality retained state exceeds Engine operator-state budget"
            ))
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn q16_cancel_and_deadline_abort_without_result() {
        let profile = profile_result(
            1,
            vec![text_column("t", 1, 0, Some(1), None, 0)],
            Some(0),
            false,
            1,
        );
        let engine = ExecutionEngine::new(ConnectorRegistry::new());

        let cancelled_context =
            RequestContext::with_deadline(Instant::now() + Duration::from_secs(60));
        cancelled_context.cancellation().cancel();
        let cancelled = QualityRequest::new(
            profile.clone(),
            cancelled_context,
            provenance(Uuid::from_u128(16)),
        )
        .expect("request");
        assert!(matches!(
            engine.quality(cancelled).await,
            Err(EngineError::Cancelled)
        ));

        let deadline_context = RequestContext::with_deadline(Instant::now());
        let expired =
            QualityRequest::new(profile, deadline_context, provenance(Uuid::from_u128(16)))
                .expect("request");
        assert!(matches!(
            engine.quality(expired).await,
            Err(EngineError::Timeout)
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn q17_existing_engine_run_gate_is_reused() {
        let profile = profile_result(
            1,
            vec![text_column("t", 1, 0, Some(1), None, 0)],
            Some(0),
            false,
            1,
        );
        let engine = ExecutionEngine::new(ConnectorRegistry::new());
        let mut permits = Vec::new();
        for _ in 0..crate::MAX_ENGINE_CONCURRENT_RUNS {
            permits.push(engine.try_hold_run_gate().expect("permit"));
        }
        let request = QualityRequest::new(profile, context(), provenance(Uuid::from_u128(17)))
            .expect("request");
        assert!(matches!(
            engine.quality(request).await,
            Err(EngineError::Busy)
        ));
        drop(permits);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn q18_qr1_profile_contract_and_digest_are_fail_closed_unchanged() {
        let mut profile = profile_result(
            1,
            vec![text_column("t", 1, 0, Some(1), None, 0)],
            Some(0),
            false,
            1,
        );
        run_quality(profile.clone(), Uuid::from_u128(18))
            .await
            .expect("valid profile");
        profile.profile.profiling_contract_version += 1;
        let error = run_quality(profile, Uuid::from_u128(18))
            .await
            .expect_err("unknown profile version");
        assert!(matches!(
            error,
            EngineError::InvalidPlan("unknown profile report or profiling contract version")
        ));
    }
}
