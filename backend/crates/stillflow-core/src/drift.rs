//! Q-D1 Profile History / Drift value contracts.
//!
//! These values are transport-neutral. Storage owns lifecycle persistence and
//! Engine owns comparison; the closed enums and exact rational policy live in
//! Core so neither layer can silently invent a second semantic authority.

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const PROFILE_HISTORY_DRIFT_CONTRACT_VERSION: u16 = 1;
pub const DRIFT_THRESHOLD_POLICY_VERSION: u16 = 1;
pub const DRIFT_MAX_HISTORY_PAGE_SIZE: usize = 100;
pub const DRIFT_MAX_HISTORY_REFERENCE_BYTES: usize = 1_048_576;
pub const DRIFT_MAX_HISTORY_FILTER_COLUMNS: usize = 256;
pub const DRIFT_MAX_PROFILES_PER_COMPARISON: usize = 2;
pub const DRIFT_MAX_COMPARE_COLUMNS: usize = 256;
pub const DRIFT_MAX_FINDINGS_PER_REPORT: usize = 4_096;
pub const DRIFT_MAX_MISSING_METRICS: usize = 256;
pub const DRIFT_MAX_EVIDENCE_REFS_PER_FINDING: usize = 8;
pub const DRIFT_MAX_RETAINED_EVIDENCE_BYTES_PER_FINDING: usize = 65_536;
pub const DRIFT_MAX_REPORT_BYTES: usize = 2 * 1024 * 1024;
pub const DRIFT_MAX_REPORT_PAGE_SIZE: usize = 100;
pub const DRIFT_MINIMUM_METRIC_ROWS: u64 = 20;

/// Exact reduced rational used by every Q-D1 threshold and observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DriftRational {
    pub numerator: i128,
    pub denominator: u128,
}

impl DriftRational {
    pub fn new(numerator: i128, denominator: u128) -> Option<Self> {
        if denominator == 0 {
            return None;
        }
        let gcd = gcd_u128(numerator.unsigned_abs(), denominator);
        let gcd = if gcd == 0 { 1 } else { gcd };
        Some(Self {
            numerator: numerator / i128::try_from(gcd).ok()?,
            denominator: denominator / gcd,
        })
    }

    pub const fn zero() -> Self {
        Self {
            numerator: 0,
            denominator: 1,
        }
    }

    pub fn cmp_value(self, other: Self) -> Option<Ordering> {
        self.numerator
            .checked_mul(i128::try_from(other.denominator).ok()?)
            .and_then(|left| {
                other
                    .numerator
                    .checked_mul(i128::try_from(self.denominator).ok()?)
                    .map(|right| left.cmp(&right))
            })
    }

    pub fn abs_delta(left: Self, right: Self) -> Option<Self> {
        let left_term = left
            .numerator
            .checked_mul(i128::try_from(right.denominator).ok()?)?;
        let right_term = right
            .numerator
            .checked_mul(i128::try_from(left.denominator).ok()?)?;
        let denominator = left.denominator.checked_mul(right.denominator)?;
        Self::new((left_term - right_term).abs(), denominator)
    }
}

fn gcd_u128(mut left: u128, mut right: u128) -> u128 {
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub enum DriftBaselineMode {
    Explicit(Uuid),
    LatestEligible,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DriftObservationWindow {
    pub start_sequence: u64,
    pub end_sequence: u64,
}

impl DriftObservationWindow {
    pub fn validate(&self) -> bool {
        self.start_sequence < self.end_sequence
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DriftComparisonRequest {
    pub workspace_id: Uuid,
    pub dataset_id: Uuid,
    pub candidate_history_id: Uuid,
    pub baseline: DriftBaselineMode,
    pub threshold_policy_version: u16,
    pub observation_window: Option<DriftObservationWindow>,
    pub report_contract_version: u16,
}

impl DriftComparisonRequest {
    pub fn validate(&self) -> bool {
        !self.workspace_id.is_nil()
            && !self.dataset_id.is_nil()
            && !self.candidate_history_id.is_nil()
            && self.threshold_policy_version == DRIFT_THRESHOLD_POLICY_VERSION
            && self.report_contract_version == PROFILE_HISTORY_DRIFT_CONTRACT_VERSION
            && self
                .observation_window
                .as_ref()
                .is_none_or(DriftObservationWindow::validate)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DriftOutcome {
    Complete,
    Partial,
    NoBaseline,
    IncompatibleVersion,
    TombstonedInput,
    InvalidComparison,
    OutputLimitExceeded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DriftMissingReason {
    NoBaseline,
    NoRows,
    TruncatedScan,
    UnsupportedType,
    MetricAbsent,
    TooFewRows,
    IncompatibleSchema,
    TombstonedInput,
    IncompatibleVersion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DriftFindingKind {
    SchemaColumnAdded,
    SchemaColumnRemoved,
    SchemaColumnTypeChanged,
    SchemaColumnNullabilityChanged,
    DistributionNumericHistogramL1Exceeded,
    DistributionNullRateDeltaExceeded,
}

impl DriftFindingKind {
    pub const fn rank(self) -> u8 {
        match self {
            Self::SchemaColumnAdded => 0,
            Self::SchemaColumnRemoved => 1,
            Self::SchemaColumnTypeChanged => 2,
            Self::SchemaColumnNullabilityChanged => 3,
            Self::DistributionNumericHistogramL1Exceeded => 4,
            Self::DistributionNullRateDeltaExceeded => 5,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DriftMissingMetric {
    pub column_name: String,
    pub metric_path: String,
    pub reason: DriftMissingReason,
}
