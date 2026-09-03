//! Deterministic Q-D1 Profile History / Drift comparison.
//!
//! The comparator consumes committed profile bodies and Dataset-owned history
//! metadata. It does not scan source data, read a clock, select a baseline by
//! insertion time, or retain raw cell values.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use crate::{EngineError, ExecutionEngine};
use serde_json::{Map, Number, Value};
use sha2::{Digest as _, Sha256};
use stillflow_core::{
    DriftBaselineMode, DriftComparisonRequest, DriftFindingKind, DriftMissingMetric,
    DriftMissingReason, DriftObservationWindow, DriftOutcome, DriftRational, FindingSeverity,
    LogicalField, LogicalType, DRIFT_MAX_COMPARE_COLUMNS, DRIFT_MAX_EVIDENCE_REFS_PER_FINDING,
    DRIFT_MAX_FINDINGS_PER_REPORT, DRIFT_MAX_MISSING_METRICS, DRIFT_MAX_REPORT_BYTES,
    DRIFT_MAX_RETAINED_EVIDENCE_BYTES_PER_FINDING, DRIFT_MINIMUM_METRIC_ROWS,
    DRIFT_THRESHOLD_POLICY_VERSION, PROFILE_HISTORY_DRIFT_CONTRACT_VERSION,
};
use stillflow_storage::{ProfileHistoryEntry, ProfileHistoryState};

pub const DRIFT_DETECTOR_ID: &str = "q-d1-v1";

#[derive(Debug, Clone)]
pub struct DriftProfileInput {
    pub entry: ProfileHistoryEntry,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct DriftRequest {
    pub comparison: DriftComparisonRequest,
    pub baseline: Option<DriftProfileInput>,
    pub candidate: DriftProfileInput,
    pub context: stillflow_core::RequestContext,
}

#[derive(Debug, Clone)]
pub struct DriftFinding {
    pub finding_id: String,
    pub kind: DriftFindingKind,
    pub detector_id: &'static str,
    pub detector_contract_version: u16,
    pub severity: FindingSeverity,
    pub column_name: String,
    pub metric_path: String,
    pub observed: Option<DriftRational>,
    pub threshold: Option<DriftRational>,
    pub evidence: Vec<Value>,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct DriftReport {
    pub artifact_type: &'static str,
    pub artifact_body_version: u16,
    pub profile_history_drift_contract_version: u16,
    pub baseline_profile_digest: String,
    pub candidate_profile_digest: String,
    pub threshold_policy_version: u16,
    pub numeric_histogram_l1_threshold: DriftRational,
    pub null_rate_delta_threshold: DriftRational,
    pub observation_window: Option<DriftObservationWindow>,
    pub outcome: DriftOutcome,
    pub completeness: bool,
    pub missing_metrics: Vec<DriftMissingMetric>,
    pub findings: Vec<DriftFinding>,
    pub canonical_input_digest: String,
}

#[derive(Debug, Clone)]
pub struct DriftResult {
    pub outcome: DriftOutcome,
    pub report: Option<DriftReport>,
    pub canonical_body: Option<Vec<u8>>,
    pub canonical_digest: Option<String>,
    pub canonical_input_digest: String,
}

impl ExecutionEngine {
    /// Resolves a Dataset-owned history comparison through the existing
    /// committed Artifact body and bounded history readers, then executes the
    /// comparison under the normal Engine run gate.
    pub async fn drift_history(
        &self,
        store: &stillflow_storage::ControlPlaneStore,
        comparison: DriftComparisonRequest,
        context: stillflow_core::RequestContext,
    ) -> Result<DriftResult, EngineError> {
        if context.deadline().is_none() {
            return Err(EngineError::InvalidPlan(
                "drift run requires a request deadline",
            ));
        }
        context
            .ensure_active()
            .map_err(|error| match error.category() {
                stillflow_core::ErrorCategory::Cancelled => EngineError::Cancelled,
                stillflow_core::ErrorCategory::Timeout => EngineError::Timeout,
                _ => EngineError::InvalidPlan("drift request context is invalid"),
            })?;
        let candidate = store
            .get_profile_history(
                comparison.workspace_id,
                comparison.dataset_id,
                comparison.candidate_history_id,
            )
            .map_err(EngineError::Storage)?;
        let baseline = store
            .select_profile_history_baseline(
                comparison.workspace_id,
                comparison.dataset_id,
                comparison.candidate_history_id,
                comparison.baseline,
                comparison.observation_window,
            )
            .map_err(EngineError::Storage)?
            .map(|entry| {
                store
                    .get_artifact_body(entry.profile_artifact_id)
                    .map(|body| (entry, body))
            })
            .transpose()
            .map_err(EngineError::Storage)?
            .map(|(entry, body)| DriftProfileInput {
                entry,
                body: body.body,
            });
        let body = store
            .get_artifact_body(candidate.profile_artifact_id)
            .map_err(EngineError::Storage)?;
        self.drift(DriftRequest {
            comparison,
            baseline,
            candidate: DriftProfileInput {
                entry: candidate,
                body: body.body,
            },
            context,
        })
        .await
    }

    /// Runs one bounded deterministic comparison through the existing Engine
    /// run gate. The comparison consumes two already committed profile inputs.
    pub async fn drift(&self, request: DriftRequest) -> Result<DriftResult, EngineError> {
        if request.context.deadline().is_none() {
            return Err(EngineError::InvalidPlan(
                "drift run requires a request deadline",
            ));
        }
        request
            .candidate
            .entry
            .workspace_id
            .eq(&request.comparison.workspace_id)
            .then_some(())
            .ok_or(EngineError::InvalidPlan(
                "candidate is outside comparison scope",
            ))?;
        let permit = self.try_acquire_run_permit()?;
        let result = self.drift_with_permit(request).await;
        drop(permit);
        result
    }

    /// Runs while the caller owns the single JobRuntime Engine permit.
    pub(crate) async fn drift_with_permit(
        &self,
        request: DriftRequest,
    ) -> Result<DriftResult, EngineError> {
        request
            .candidate
            .entry
            .workspace_id
            .eq(&request.comparison.workspace_id)
            .then_some(())
            .ok_or(EngineError::InvalidPlan(
                "candidate is outside comparison scope",
            ))?;
        request
            .candidate
            .entry
            .dataset_id
            .eq(&request.comparison.dataset_id)
            .then_some(())
            .ok_or(EngineError::InvalidPlan(
                "candidate is outside comparison scope",
            ))?;
        request
            .context
            .ensure_active()
            .map_err(|error| match error.category() {
                stillflow_core::ErrorCategory::Cancelled => EngineError::Cancelled,
                stillflow_core::ErrorCategory::Timeout => EngineError::Timeout,
                _ => EngineError::InvalidPlan("drift request context is invalid"),
            })?;
        compare(request)
    }
}

fn compare(request: DriftRequest) -> Result<DriftResult, EngineError> {
    if !request.comparison.validate() {
        return Ok(no_report(DriftOutcome::InvalidComparison, [0; 32]));
    }
    let candidate = &request.candidate;
    if candidate.entry.history_id != request.comparison.candidate_history_id {
        return Ok(no_report(DriftOutcome::InvalidComparison, [0; 32]));
    }
    let Some(baseline) = request.baseline.as_ref() else {
        return Ok(no_report(DriftOutcome::NoBaseline, [0; 32]));
    };
    if baseline.entry.state != ProfileHistoryState::Active
        || candidate.entry.state != ProfileHistoryState::Active
    {
        return Ok(no_report(DriftOutcome::TombstonedInput, [0; 32]));
    }
    if baseline.entry.workspace_id != candidate.entry.workspace_id
        || baseline.entry.dataset_id != candidate.entry.dataset_id
        || baseline.entry.workspace_id != request.comparison.workspace_id
        || baseline.entry.dataset_id != request.comparison.dataset_id
        || baseline.entry.history_id == candidate.entry.history_id
        || baseline.entry.profile_sequence >= candidate.entry.profile_sequence
    {
        return Ok(no_report(DriftOutcome::InvalidComparison, [0; 32]));
    }
    if let DriftBaselineMode::Explicit(history_id) = request.comparison.baseline {
        if history_id != baseline.entry.history_id {
            return Ok(no_report(DriftOutcome::InvalidComparison, [0; 32]));
        }
    }
    if let Some(window) = request.comparison.observation_window {
        if !window.validate()
            || baseline.entry.profile_sequence < window.start_sequence
            || baseline.entry.profile_sequence >= window.end_sequence
            || candidate.entry.profile_sequence < window.start_sequence
            || candidate.entry.profile_sequence >= window.end_sequence
        {
            return Ok(no_report(DriftOutcome::InvalidComparison, [0; 32]));
        }
    }
    if !compatible(&baseline.entry, &candidate.entry) {
        return Ok(no_report(DriftOutcome::IncompatibleVersion, [0; 32]));
    }

    let baseline_digest = hex(&baseline.entry.profile_digest);
    let candidate_digest = hex(&candidate.entry.profile_digest);
    let input_digest = comparison_digest(&request.comparison, &baseline_digest, &candidate_digest);
    let baseline_profile = parse_profile(baseline)?;
    let candidate_profile = parse_profile(candidate)?;
    let mut findings = Vec::new();
    let mut missing_metrics = Vec::new();

    let baseline_fields = schema_fields(&baseline.entry.schema);
    let candidate_fields = schema_fields(&candidate.entry.schema);
    let mut names = BTreeSet::new();
    names.extend(baseline_fields.keys().cloned());
    names.extend(candidate_fields.keys().cloned());
    if names.len() > DRIFT_MAX_COMPARE_COLUMNS {
        return Ok(no_report(DriftOutcome::OutputLimitExceeded, input_digest));
    }

    let mut names = names.into_iter().collect::<Vec<_>>();
    names.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    for name in names {
        match (baseline_fields.get(&name), candidate_fields.get(&name)) {
            (None, Some(candidate_field)) => findings.push(schema_finding(
                DriftFindingKind::SchemaColumnAdded,
                FindingSeverity::Warning,
                &name,
                "candidate schema contains a new column",
                None,
                &baseline_digest,
                &candidate_digest,
                Some(type_name(&candidate_field.data_type)),
            )),
            (Some(baseline_field), None) => findings.push(schema_finding(
                DriftFindingKind::SchemaColumnRemoved,
                FindingSeverity::Warning,
                &name,
                "candidate schema no longer contains a baseline column",
                Some(type_name(&baseline_field.data_type)),
                &baseline_digest,
                &candidate_digest,
                None,
            )),
            (Some(baseline_field), Some(candidate_field)) => {
                let baseline_type = type_name(&baseline_field.data_type);
                let candidate_type = type_name(&candidate_field.data_type);
                let type_changed = baseline_type != candidate_type;
                if type_changed {
                    findings.push(schema_finding(
                        DriftFindingKind::SchemaColumnTypeChanged,
                        FindingSeverity::Error,
                        &name,
                        "candidate column type differs from baseline",
                        Some(baseline_type.clone()),
                        &baseline_digest,
                        &candidate_digest,
                        Some(candidate_type.clone()),
                    ));
                }
                if baseline_field.nullable != candidate_field.nullable {
                    findings.push(schema_finding(
                        DriftFindingKind::SchemaColumnNullabilityChanged,
                        FindingSeverity::Warning,
                        &name,
                        "candidate column nullability differs from baseline",
                        Some(nullable_text(baseline_field.nullable)),
                        &baseline_digest,
                        &candidate_digest,
                        Some(nullable_text(candidate_field.nullable)),
                    ));
                }
                compare_metrics(
                    &name,
                    type_changed,
                    baseline_profile.columns.get(&name),
                    candidate_profile.columns.get(&name),
                    &baseline_digest,
                    &candidate_digest,
                    baseline_profile.rows,
                    candidate_profile.rows,
                    baseline.entry.truncated,
                    candidate.entry.truncated,
                    &mut findings,
                    &mut missing_metrics,
                );
            }
            (None, None) => unreachable!(),
        }
    }

    missing_metrics.sort_by(|left, right| {
        left.column_name
            .as_bytes()
            .cmp(right.column_name.as_bytes())
            .then_with(|| {
                left.metric_path
                    .as_bytes()
                    .cmp(right.metric_path.as_bytes())
            })
            .then_with(|| format!("{:?}", left.reason).cmp(&format!("{:?}", right.reason)))
    });
    if missing_metrics.len() > DRIFT_MAX_MISSING_METRICS {
        return Ok(no_report(DriftOutcome::OutputLimitExceeded, input_digest));
    }
    findings.sort_by(finding_order);
    if findings.len() > DRIFT_MAX_FINDINGS_PER_REPORT
        || findings.iter().any(|finding| {
            finding.evidence.len() > DRIFT_MAX_EVIDENCE_REFS_PER_FINDING
                || canonical_json(&Value::Array(finding.evidence.clone())).len()
                    > DRIFT_MAX_RETAINED_EVIDENCE_BYTES_PER_FINDING
        })
    {
        return Ok(no_report(DriftOutcome::OutputLimitExceeded, input_digest));
    }
    let report = DriftReport {
        artifact_type: "drift_report.v1",
        artifact_body_version: 1,
        profile_history_drift_contract_version: PROFILE_HISTORY_DRIFT_CONTRACT_VERSION,
        baseline_profile_digest: baseline_digest,
        candidate_profile_digest: candidate_digest,
        threshold_policy_version: DRIFT_THRESHOLD_POLICY_VERSION,
        numeric_histogram_l1_threshold: DriftRational::new(1, 5).expect("fixed threshold"),
        null_rate_delta_threshold: DriftRational::new(1, 10).expect("fixed threshold"),
        observation_window: request.comparison.observation_window,
        outcome: if missing_metrics.is_empty() {
            DriftOutcome::Complete
        } else {
            DriftOutcome::Partial
        },
        completeness: missing_metrics.is_empty(),
        missing_metrics,
        findings,
        canonical_input_digest: hex(&input_digest),
    };
    let body = report.canonical_body();
    if body.len() > DRIFT_MAX_REPORT_BYTES {
        return Ok(no_report(DriftOutcome::OutputLimitExceeded, input_digest));
    }
    let digest: [u8; 32] = Sha256::digest(&body).into();
    Ok(DriftResult {
        outcome: report.outcome,
        report: Some(report),
        canonical_body: Some(body),
        canonical_digest: Some(hex(&digest)),
        canonical_input_digest: hex(&input_digest),
    })
}

#[derive(Debug)]
struct ParsedProfile {
    rows: u64,
    columns: BTreeMap<String, ParsedColumn>,
}

#[derive(Debug)]
struct ParsedColumn {
    logical_type: String,
    status: String,
    null_count: Option<u64>,
    non_null_count: Option<u64>,
    histogram: Option<Vec<u64>>,
}

fn parse_profile(input: &DriftProfileInput) -> Result<ParsedProfile, EngineError> {
    let digest: [u8; 32] = Sha256::digest(&input.body).into();
    if digest != input.entry.profile_digest {
        return Err(EngineError::InvalidPlan(
            "ProfileHistory body digest does not match its entry",
        ));
    }
    let value: Value = serde_json::from_slice(&input.body)
        .map_err(|_| EngineError::InvalidPlan("ProfileHistory profile body is not JSON"))?;
    if value.get("artifact_type").and_then(Value::as_str) != Some("profile_report")
        || value.get("artifact_body_version").and_then(Value::as_u64) != Some(1)
        || value
            .get("profiling_contract_version")
            .and_then(Value::as_u64)
            != Some(u64::from(input.entry.profile_contract_version))
    {
        return Err(EngineError::InvalidPlan(
            "ProfileHistory profile body version is incompatible",
        ));
    }
    let dataset =
        value
            .get("dataset")
            .and_then(Value::as_object)
            .ok_or(EngineError::InvalidPlan(
                "ProfileHistory profile dataset is missing",
            ))?;
    let rows = dataset
        .get("row_count_scanned")
        .and_then(Value::as_u64)
        .ok_or(EngineError::InvalidPlan(
            "ProfileHistory row count is missing",
        ))?;
    if rows > crate::PROFILE_MAX_ROWS as u64 {
        return Err(EngineError::BoundExceeded(
            "ProfileHistory profile rows exceed the profiling bound",
        ));
    }
    let columns =
        value
            .get("columns")
            .and_then(Value::as_array)
            .ok_or(EngineError::InvalidPlan(
                "ProfileHistory profile columns are missing",
            ))?;
    if columns.len() > DRIFT_MAX_COMPARE_COLUMNS {
        return Err(EngineError::BoundExceeded(
            "ProfileHistory profile columns exceed the comparison bound",
        ));
    }
    let mut parsed = BTreeMap::new();
    for column in columns {
        let column = column.as_object().ok_or(EngineError::InvalidPlan(
            "ProfileHistory column is not an object",
        ))?;
        let name = column
            .get("name")
            .and_then(Value::as_str)
            .ok_or(EngineError::InvalidPlan(
                "ProfileHistory column name is missing",
            ))?;
        let parsed_column = ParsedColumn {
            logical_type: column
                .get("type")
                .and_then(Value::as_str)
                .ok_or(EngineError::InvalidPlan(
                    "ProfileHistory column type is missing",
                ))?
                .to_owned(),
            status: column
                .get("status")
                .and_then(Value::as_str)
                .ok_or(EngineError::InvalidPlan(
                    "ProfileHistory column status is missing",
                ))?
                .to_owned(),
            null_count: column.get("null_count").and_then(Value::as_u64),
            non_null_count: column.get("non_null_count").and_then(Value::as_u64),
            histogram: column.get("histogram").and_then(histogram_counts),
        };
        validate_profile_column_metrics(&parsed_column, rows)?;
        if parsed.insert(name.to_owned(), parsed_column).is_some() {
            return Err(EngineError::InvalidPlan(
                "ProfileHistory profile has duplicate columns",
            ));
        }
    }
    Ok(ParsedProfile {
        rows,
        columns: parsed,
    })
}

fn validate_profile_column_metrics(column: &ParsedColumn, rows: u64) -> Result<(), EngineError> {
    if column.null_count.is_some_and(|count| count > rows)
        || column.non_null_count.is_some_and(|count| count > rows)
    {
        return Err(EngineError::InvalidPlan(
            "ProfileHistory column counts exceed its row count",
        ));
    }
    if let Some(histogram) = column.histogram.as_ref() {
        let total = histogram
            .iter()
            .try_fold(0_u64, |total, count| total.checked_add(*count));
        if total.is_none()
            || total.is_some_and(|total| total > column.non_null_count.unwrap_or(rows))
        {
            return Err(EngineError::InvalidPlan(
                "ProfileHistory histogram counts exceed its non-null count",
            ));
        }
    }
    Ok(())
}

fn histogram_counts(value: &Value) -> Option<Vec<u64>> {
    let array = value
        .as_array()
        .or_else(|| value.get("counts").and_then(Value::as_array))?;
    array.iter().map(Value::as_u64).collect()
}

#[allow(clippy::too_many_arguments)]
fn compare_metrics(
    name: &str,
    type_changed: bool,
    baseline: Option<&ParsedColumn>,
    candidate: Option<&ParsedColumn>,
    baseline_digest: &str,
    candidate_digest: &str,
    baseline_rows: u64,
    candidate_rows: u64,
    baseline_truncated: bool,
    candidate_truncated: bool,
    findings: &mut Vec<DriftFinding>,
    missing: &mut Vec<DriftMissingMetric>,
) {
    let metric_path = "columns.metrics";
    let Some(baseline) = baseline else {
        missing.push(missing_metric(
            name,
            metric_path,
            DriftMissingReason::MetricAbsent,
        ));
        return;
    };
    let Some(candidate) = candidate else {
        missing.push(missing_metric(
            name,
            metric_path,
            DriftMissingReason::MetricAbsent,
        ));
        return;
    };
    if type_changed {
        missing.push(missing_metric(
            name,
            metric_path,
            DriftMissingReason::IncompatibleSchema,
        ));
        return;
    }
    let unavailable = |column: &ParsedColumn, rows: u64, truncated: bool| {
        if truncated {
            Some(DriftMissingReason::TruncatedScan)
        } else if rows == 0 {
            Some(DriftMissingReason::NoRows)
        } else if column.status == "skipped_unsupported_type" {
            Some(DriftMissingReason::UnsupportedType)
        } else if column.null_count.is_none() || column.non_null_count.is_none() {
            Some(DriftMissingReason::MetricAbsent)
        } else {
            None
        }
    };
    if let Some(reason) = unavailable(baseline, baseline_rows, baseline_truncated) {
        missing.push(missing_metric(name, "columns.null_rate", reason));
    } else if let Some(reason) = unavailable(candidate, candidate_rows, candidate_truncated) {
        missing.push(missing_metric(name, "columns.null_rate", reason));
    } else if let (Some(left_nulls), Some(right_nulls)) =
        (baseline.null_count, candidate.null_count)
    {
        let left = DriftRational::new(i128::from(left_nulls), u128::from(baseline_rows));
        let right = DriftRational::new(i128::from(right_nulls), u128::from(candidate_rows));
        if let (Some(left), Some(right)) = (left, right) {
            if let Some(observed) = DriftRational::abs_delta(left, right) {
                let threshold = DriftRational::new(1, 10).expect("fixed threshold");
                if observed.cmp_value(threshold) == Some(Ordering::Greater) {
                    findings.push(metric_finding(
                        DriftFindingKind::DistributionNullRateDeltaExceeded,
                        name,
                        "columns.null_rate_delta",
                        observed,
                        threshold,
                        baseline_digest,
                        candidate_digest,
                        None,
                    ));
                }
            }
        }
    }

    let numeric =
        is_numeric_type(&baseline.logical_type) && is_numeric_type(&candidate.logical_type);
    if !numeric {
        return;
    }
    let Some(left_histogram) = baseline.histogram.as_ref() else {
        missing.push(missing_metric(
            name,
            "columns.histogram",
            DriftMissingReason::MetricAbsent,
        ));
        return;
    };
    let Some(right_histogram) = candidate.histogram.as_ref() else {
        missing.push(missing_metric(
            name,
            "columns.histogram",
            DriftMissingReason::MetricAbsent,
        ));
        return;
    };
    if left_histogram.len() != right_histogram.len() {
        missing.push(missing_metric(
            name,
            "columns.histogram",
            DriftMissingReason::IncompatibleVersion,
        ));
        return;
    }
    let left_total = left_histogram
        .iter()
        .try_fold(0_u64, |total, count| total.checked_add(*count));
    let right_total = right_histogram
        .iter()
        .try_fold(0_u64, |total, count| total.checked_add(*count));
    let (Some(left_total), Some(right_total)) = (left_total, right_total) else {
        missing.push(missing_metric(
            name,
            "columns.histogram",
            DriftMissingReason::MetricAbsent,
        ));
        return;
    };
    if left_total == 0
        || right_total == 0
        || baseline.non_null_count.unwrap_or(0) < DRIFT_MINIMUM_METRIC_ROWS
        || candidate.non_null_count.unwrap_or(0) < DRIFT_MINIMUM_METRIC_ROWS
    {
        missing.push(missing_metric(
            name,
            "columns.histogram",
            if left_total == 0 || right_total == 0 {
                DriftMissingReason::MetricAbsent
            } else {
                DriftMissingReason::TooFewRows
            },
        ));
        return;
    }
    let Some(observed) = histogram_l1(left_histogram, right_histogram) else {
        missing.push(missing_metric(
            name,
            "columns.histogram",
            DriftMissingReason::MetricAbsent,
        ));
        return;
    };
    let threshold = DriftRational::new(1, 5).expect("fixed threshold");
    if observed.cmp_value(threshold) == Some(Ordering::Greater) {
        findings.push(metric_finding(
            DriftFindingKind::DistributionNumericHistogramL1Exceeded,
            name,
            "columns.histogram_l1",
            observed,
            threshold,
            baseline_digest,
            candidate_digest,
            Some((left_histogram, right_histogram)),
        ));
    }
}

fn histogram_l1(left: &[u64], right: &[u64]) -> Option<DriftRational> {
    if left.len() != right.len() {
        return None;
    }
    let left_total: u128 = left.iter().map(|value| u128::from(*value)).sum();
    let right_total: u128 = right.iter().map(|value| u128::from(*value)).sum();
    if left_total == 0 || right_total == 0 {
        return None;
    }
    let mut numerator = 0_u128;
    for (left, right) in left.iter().zip(right) {
        let left_term = u128::from(*left).checked_mul(right_total)?;
        let right_term = u128::from(*right).checked_mul(left_total)?;
        numerator = numerator.checked_add(left_term.abs_diff(right_term))?;
    }
    DriftRational::new(
        i128::try_from(numerator).ok()?,
        left_total.checked_mul(right_total)?.checked_mul(2)?,
    )
}

#[allow(clippy::too_many_arguments)]
fn metric_finding(
    kind: DriftFindingKind,
    name: &str,
    metric_path: &str,
    observed: DriftRational,
    threshold: DriftRational,
    baseline_digest: &str,
    candidate_digest: &str,
    histograms: Option<(&[u64], &[u64])>,
) -> DriftFinding {
    let mut evidence = vec![json_object([
        ("kind", Value::String("metric".to_owned())),
        ("metric_path", Value::String(metric_path.to_owned())),
        ("observed", rational_value(observed)),
        ("threshold", rational_value(threshold)),
        (
            "baseline_profile_digest",
            Value::String(baseline_digest.to_owned()),
        ),
        (
            "candidate_profile_digest",
            Value::String(candidate_digest.to_owned()),
        ),
    ])];
    if let Some((baseline, candidate)) = histograms {
        evidence.push(json_object([
            ("kind", Value::String("histogram".to_owned())),
            (
                "baseline_counts",
                Value::Array(baseline.iter().map(|v| Value::from(*v)).collect()),
            ),
            (
                "candidate_counts",
                Value::Array(candidate.iter().map(|v| Value::from(*v)).collect()),
            ),
        ]));
    }
    finding(
        kind,
        FindingSeverity::Error,
        name,
        metric_path,
        Some(observed),
        Some(threshold),
        evidence,
        baseline_digest,
        candidate_digest,
        "deterministic drift threshold exceeded",
    )
}

#[allow(clippy::too_many_arguments)]
fn schema_finding(
    kind: DriftFindingKind,
    severity: FindingSeverity,
    name: &str,
    message: &str,
    baseline_value: Option<String>,
    baseline_digest: &str,
    candidate_digest: &str,
    candidate_value: Option<String>,
) -> DriftFinding {
    let mut evidence = vec![json_object([
        ("kind", Value::String("schema".to_owned())),
        (
            "baseline_profile_digest",
            Value::String(baseline_digest.to_owned()),
        ),
        (
            "candidate_profile_digest",
            Value::String(candidate_digest.to_owned()),
        ),
    ])];
    if let Some(value) = baseline_value {
        evidence[0]["baseline"] = Value::String(value);
    }
    if let Some(value) = candidate_value {
        evidence[0]["candidate"] = Value::String(value);
    }
    finding(
        kind,
        severity,
        name,
        "schema",
        None,
        None,
        evidence,
        baseline_digest,
        candidate_digest,
        message,
    )
}

#[allow(clippy::too_many_arguments)]
fn finding(
    kind: DriftFindingKind,
    severity: FindingSeverity,
    name: &str,
    metric_path: &str,
    observed: Option<DriftRational>,
    threshold: Option<DriftRational>,
    evidence: Vec<Value>,
    baseline_digest: &str,
    candidate_digest: &str,
    message: &str,
) -> DriftFinding {
    let finding_id = finding_id(kind, name, baseline_digest, candidate_digest, metric_path);
    DriftFinding {
        finding_id,
        kind,
        detector_id: DRIFT_DETECTOR_ID,
        detector_contract_version: PROFILE_HISTORY_DRIFT_CONTRACT_VERSION,
        severity,
        column_name: name.to_owned(),
        metric_path: metric_path.to_owned(),
        observed,
        threshold,
        evidence,
        message: message.to_owned(),
    }
}

fn finding_id(
    kind: DriftFindingKind,
    name: &str,
    baseline_digest: &str,
    candidate_digest: &str,
    metric_path: &str,
) -> String {
    let mut bytes = Vec::new();
    for value in [
        kind_text(kind),
        name,
        baseline_digest,
        candidate_digest,
        metric_path,
    ] {
        bytes.extend_from_slice(&(value.len() as u64).to_be_bytes());
        bytes.extend_from_slice(value.as_bytes());
    }
    let digest: [u8; 32] = Sha256::digest(bytes).into();
    hex(&digest)
}

fn finding_order(left: &DriftFinding, right: &DriftFinding) -> Ordering {
    left.kind
        .rank()
        .cmp(&right.kind.rank())
        .then_with(|| {
            left.column_name
                .as_bytes()
                .cmp(right.column_name.as_bytes())
        })
        .then_with(|| {
            left.metric_path
                .as_bytes()
                .cmp(right.metric_path.as_bytes())
        })
        .then_with(|| left.finding_id.as_bytes().cmp(right.finding_id.as_bytes()))
}

fn missing_metric(
    column_name: &str,
    metric_path: &str,
    reason: DriftMissingReason,
) -> DriftMissingMetric {
    DriftMissingMetric {
        column_name: column_name.to_owned(),
        metric_path: metric_path.to_owned(),
        reason,
    }
}

fn schema_fields(schema: &stillflow_core::LogicalSchema) -> BTreeMap<String, LogicalField> {
    schema
        .fields
        .iter()
        .cloned()
        .map(|field| (field.name.clone(), field))
        .collect()
}

fn type_name(data_type: &LogicalType) -> String {
    match data_type {
        LogicalType::Null => "null".to_owned(),
        LogicalType::Boolean => "boolean".to_owned(),
        LogicalType::Int8 => "int8".to_owned(),
        LogicalType::Int16 => "int16".to_owned(),
        LogicalType::Int32 => "int32".to_owned(),
        LogicalType::Int64 => "int64".to_owned(),
        LogicalType::UInt8 => "uint8".to_owned(),
        LogicalType::UInt16 => "uint16".to_owned(),
        LogicalType::UInt32 => "uint32".to_owned(),
        LogicalType::UInt64 => "uint64".to_owned(),
        LogicalType::Float32 => "float32".to_owned(),
        LogicalType::Float64 => "float64".to_owned(),
        LogicalType::Utf8 => "utf8".to_owned(),
        LogicalType::Binary => "binary".to_owned(),
        LogicalType::Date32 => "date32".to_owned(),
        LogicalType::Timestamp { unit, .. } => match unit {
            stillflow_core::TimeUnit::Second => "timestamp_s".to_owned(),
            stillflow_core::TimeUnit::Millisecond => "timestamp_ms".to_owned(),
            stillflow_core::TimeUnit::Microsecond => "timestamp_us".to_owned(),
            stillflow_core::TimeUnit::Nanosecond => "timestamp_ns".to_owned(),
        },
        LogicalType::List(_) => "list".to_owned(),
        LogicalType::Struct(_) => "struct".to_owned(),
    }
}

fn is_numeric_type(value: &str) -> bool {
    matches!(
        value,
        "int8"
            | "int16"
            | "int32"
            | "int64"
            | "uint8"
            | "uint16"
            | "uint32"
            | "uint64"
            | "float32"
            | "float64"
    )
}

fn nullable_text(value: bool) -> String {
    if value { "nullable" } else { "non_nullable" }.to_owned()
}

fn compatible(left: &ProfileHistoryEntry, right: &ProfileHistoryEntry) -> bool {
    left.profile_contract_version == right.profile_contract_version
        && left.drift_contract_version == right.drift_contract_version
        && left.profile_policy_version == right.profile_policy_version
        && left.top_k == right.top_k
        && left.histogram_buckets == right.histogram_buckets
        && left.profile_contract_version == 1
        && left.drift_contract_version == PROFILE_HISTORY_DRIFT_CONTRACT_VERSION
        && left.profile_policy_version == 1
}

fn comparison_digest(
    request: &DriftComparisonRequest,
    baseline: &str,
    candidate: &str,
) -> [u8; 32] {
    let mut value = Map::new();
    value.insert(
        "workspace_id".to_owned(),
        Value::String(request.workspace_id.to_string()),
    );
    value.insert(
        "dataset_id".to_owned(),
        Value::String(request.dataset_id.to_string()),
    );
    value.insert(
        "baseline_profile_digest".to_owned(),
        Value::String(baseline.to_owned()),
    );
    value.insert(
        "candidate_profile_digest".to_owned(),
        Value::String(candidate.to_owned()),
    );
    value.insert(
        "profile_history_drift_contract_version".to_owned(),
        Value::from(u64::from(PROFILE_HISTORY_DRIFT_CONTRACT_VERSION)),
    );
    value.insert(
        "threshold_policy_version".to_owned(),
        Value::from(u64::from(request.threshold_policy_version)),
    );
    value.insert(
        "observation_window".to_owned(),
        request
            .observation_window
            .map_or(Value::String("none".to_owned()), |window| {
                json_object([
                    ("start_sequence", Value::from(window.start_sequence)),
                    ("end_sequence", Value::from(window.end_sequence)),
                ])
            }),
    );
    Sha256::digest(canonical_json(&Value::Object(value))).into()
}

fn no_report(outcome: DriftOutcome, input_digest: [u8; 32]) -> DriftResult {
    DriftResult {
        outcome,
        report: None,
        canonical_body: None,
        canonical_digest: None,
        canonical_input_digest: hex(&input_digest),
    }
}

impl DriftReport {
    pub fn canonical_body(&self) -> Vec<u8> {
        let findings = self
            .findings
            .iter()
            .map(|finding| {
                let mut value = Map::new();
                value.insert(
                    "column_name".to_owned(),
                    Value::String(finding.column_name.clone()),
                );
                value.insert(
                    "detector_contract_version".to_owned(),
                    Value::from(u64::from(finding.detector_contract_version)),
                );
                value.insert(
                    "detector_id".to_owned(),
                    Value::String(finding.detector_id.to_owned()),
                );
                value.insert(
                    "evidence".to_owned(),
                    Value::Array(finding.evidence.clone()),
                );
                value.insert(
                    "finding_id".to_owned(),
                    Value::String(finding.finding_id.clone()),
                );
                value.insert(
                    "kind".to_owned(),
                    Value::String(kind_text(finding.kind).to_owned()),
                );
                value.insert("message".to_owned(), Value::String(finding.message.clone()));
                value.insert(
                    "metric_path".to_owned(),
                    Value::String(finding.metric_path.clone()),
                );
                value.insert(
                    "observed".to_owned(),
                    finding.observed.map_or(Value::Null, rational_value),
                );
                value.insert(
                    "severity".to_owned(),
                    Value::String(
                        match finding.severity {
                            FindingSeverity::Info => "Info",
                            FindingSeverity::Warning => "Warning",
                            FindingSeverity::Error => "Error",
                        }
                        .to_owned(),
                    ),
                );
                value.insert(
                    "threshold".to_owned(),
                    finding.threshold.map_or(Value::Null, rational_value),
                );
                Value::Object(value)
            })
            .collect();
        let missing = self
            .missing_metrics
            .iter()
            .map(|metric| {
                json_object([
                    ("column_name", Value::String(metric.column_name.clone())),
                    ("metric_path", Value::String(metric.metric_path.clone())),
                    (
                        "reason",
                        Value::String(missing_reason_text(metric.reason).to_owned()),
                    ),
                ])
            })
            .collect();
        let thresholds = json_object([
            (
                "numeric_histogram_l1",
                rational_value(self.numeric_histogram_l1_threshold),
            ),
            (
                "null_rate_delta",
                rational_value(self.null_rate_delta_threshold),
            ),
        ]);
        let mut value = Map::new();
        value.insert("artifact_body_version".to_owned(), Value::from(1_u64));
        value.insert(
            "artifact_type".to_owned(),
            Value::String(self.artifact_type.to_owned()),
        );
        value.insert(
            "baseline_profile_digest".to_owned(),
            Value::String(self.baseline_profile_digest.clone()),
        );
        value.insert(
            "candidate_profile_digest".to_owned(),
            Value::String(self.candidate_profile_digest.clone()),
        );
        value.insert(
            "canonical_input_digest".to_owned(),
            Value::String(self.canonical_input_digest.clone()),
        );
        value.insert("completeness".to_owned(), Value::Bool(self.completeness));
        value.insert("findings".to_owned(), Value::Array(findings));
        value.insert("missing_metrics".to_owned(), Value::Array(missing));
        value.insert(
            "observation_window".to_owned(),
            self.observation_window
                .map_or(Value::String("none".to_owned()), |window| {
                    json_object([
                        ("start_sequence", Value::from(window.start_sequence)),
                        ("end_sequence", Value::from(window.end_sequence)),
                    ])
                }),
        );
        value.insert(
            "outcome".to_owned(),
            Value::String(outcome_text(self.outcome).to_owned()),
        );
        value.insert(
            "profile_history_drift_contract_version".to_owned(),
            Value::from(u64::from(self.profile_history_drift_contract_version)),
        );
        value.insert(
            "threshold_policy_version".to_owned(),
            Value::from(u64::from(self.threshold_policy_version)),
        );
        value.insert("thresholds".to_owned(), thresholds);
        canonical_json(&Value::Object(value))
    }
}

fn rational_value(value: DriftRational) -> Value {
    json_object([
        (
            "denominator",
            Value::Number(Number::from(
                u64::try_from(value.denominator).expect("profile bounds keep rational denominator"),
            )),
        ),
        (
            "numerator",
            Value::Number(Number::from(
                u64::try_from(value.numerator).expect("profile bounds keep rational numerator"),
            )),
        ),
    ])
}

fn json_object<const N: usize>(entries: [(&str, Value); N]) -> Value {
    let mut object = Map::new();
    for (key, value) in entries {
        object.insert(key.to_owned(), value);
    }
    Value::Object(object)
}

fn kind_text(kind: DriftFindingKind) -> &'static str {
    match kind {
        DriftFindingKind::SchemaColumnAdded => "schema.column_added",
        DriftFindingKind::SchemaColumnRemoved => "schema.column_removed",
        DriftFindingKind::SchemaColumnTypeChanged => "schema.column_type_changed",
        DriftFindingKind::SchemaColumnNullabilityChanged => "schema.column_nullability_changed",
        DriftFindingKind::DistributionNumericHistogramL1Exceeded => {
            "distribution.numeric_histogram_l1_exceeded"
        }
        DriftFindingKind::DistributionNullRateDeltaExceeded => {
            "distribution.null_rate_delta_exceeded"
        }
    }
}

fn missing_reason_text(reason: DriftMissingReason) -> &'static str {
    match reason {
        DriftMissingReason::NoBaseline => "no_baseline",
        DriftMissingReason::NoRows => "no_rows",
        DriftMissingReason::TruncatedScan => "truncated_scan",
        DriftMissingReason::UnsupportedType => "unsupported_type",
        DriftMissingReason::MetricAbsent => "metric_absent",
        DriftMissingReason::TooFewRows => "too_few_rows",
        DriftMissingReason::IncompatibleSchema => "incompatible_schema",
        DriftMissingReason::TombstonedInput => "tombstoned_input",
        DriftMissingReason::IncompatibleVersion => "incompatible_version",
    }
}

fn outcome_text(outcome: DriftOutcome) -> &'static str {
    match outcome {
        DriftOutcome::Complete => "complete",
        DriftOutcome::Partial => "partial",
        DriftOutcome::NoBaseline => "no_baseline",
        DriftOutcome::IncompatibleVersion => "incompatible_version",
        DriftOutcome::TombstonedInput => "tombstoned_input",
        DriftOutcome::InvalidComparison => "invalid_comparison",
        DriftOutcome::OutputLimitExceeded => "output_limit_exceeded",
    }
}

fn canonical_json(value: &Value) -> Vec<u8> {
    let mut output = Vec::new();
    write_canonical(value, &mut output);
    output
}

fn write_canonical(value: &Value, output: &mut Vec<u8>) {
    match value {
        Value::Null => output.extend_from_slice(b"null"),
        Value::Bool(value) => output.extend_from_slice(if *value { b"true" } else { b"false" }),
        Value::Number(value) => output.extend_from_slice(value.to_string().as_bytes()),
        Value::String(value) => {
            output.extend_from_slice(serde_json::to_string(value).unwrap_or_default().as_bytes())
        }
        Value::Array(values) => {
            output.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                write_canonical(value, output);
            }
            output.push(b']');
        }
        Value::Object(values) => {
            output.push(b'{');
            let mut entries = values.iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                output.extend_from_slice(serde_json::to_string(key).unwrap_or_default().as_bytes());
                output.push(b':');
                write_canonical(value, output);
            }
            output.push(b'}');
        }
    }
}

fn hex(value: &[u8; 32]) -> String {
    let mut output = String::with_capacity(64);
    for byte in value {
        let _ = write!(output, "{byte:02x}");
    }
    output
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde_json::json;
    use stillflow_core::{ColumnId, LogicalSchema};
    use uuid::Uuid;

    use super::*;

    fn schema(fields: Vec<LogicalField>) -> LogicalSchema {
        LogicalSchema::new(fields).expect("valid schema")
    }

    fn profile(
        history_id: u128,
        sequence: u64,
        schema: LogicalSchema,
        histogram: &[u64],
        null_count: u64,
    ) -> DriftProfileInput {
        profile_with_counts(history_id, sequence, schema, histogram, 20, null_count, 20)
    }

    fn profile_with_counts(
        history_id: u128,
        sequence: u64,
        schema: LogicalSchema,
        histogram: &[u64],
        row_count: u64,
        null_count: u64,
        non_null_count: u64,
    ) -> DriftProfileInput {
        let columns = schema
            .fields
            .iter()
            .map(|field| {
                json!({
                    "name": field.name,
                    "type": type_name(&field.data_type),
                    "status": "profiled",
                    "null_count": null_count,
                    "non_null_count": non_null_count,
                    "histogram": histogram,
                })
            })
            .collect::<Vec<_>>();
        let body = serde_json::to_vec(&json!({
            "artifact_type": "profile_report",
            "artifact_body_version": 1,
            "profiling_contract_version": 1,
            "dataset": {
                "column_count_profiled": columns.len(),
                "full_row_distinct_overflow": false,
                "row_count_scanned": row_count,
                "truncated": false,
            },
            "columns": columns,
        }))
        .expect("profile body");
        let digest: [u8; 32] = Sha256::digest(&body).into();
        let workspace_id = Uuid::from_u128(1);
        let dataset_id = Uuid::from_u128(2);
        DriftProfileInput {
            entry: ProfileHistoryEntry {
                history_id: Uuid::from_u128(history_id),
                workspace_id,
                dataset_id,
                profile_artifact_id: Uuid::from_u128(history_id + 1000),
                producing_run_id: Uuid::from_u128(history_id + 2000),
                profile_digest: digest,
                profile_contract_version: 1,
                drift_contract_version: 1,
                profile_policy_version: 1,
                top_k: 20,
                histogram_buckets: histogram.len(),
                schema_fingerprint: [7; 32],
                schema,
                row_count_scanned: row_count,
                scanned_bytes: 100,
                truncated: false,
                profile_sequence: sequence,
                state: ProfileHistoryState::Active,
                created_at: Utc::now(),
                tombstoned_at: None,
            },
            body,
        }
    }

    fn request(
        candidate: &DriftProfileInput,
        baseline: &DriftProfileInput,
    ) -> DriftComparisonRequest {
        DriftComparisonRequest {
            workspace_id: candidate.entry.workspace_id,
            dataset_id: candidate.entry.dataset_id,
            candidate_history_id: candidate.entry.history_id,
            baseline: DriftBaselineMode::Explicit(baseline.entry.history_id),
            threshold_policy_version: DRIFT_THRESHOLD_POLICY_VERSION,
            observation_window: None,
            report_contract_version: PROFILE_HISTORY_DRIFT_CONTRACT_VERSION,
        }
    }

    #[test]
    fn numeric_l1_uses_exact_strict_threshold() {
        let field = LogicalField::new(
            ColumnId::from_uuid(Uuid::from_u128(11)),
            "value",
            LogicalType::Int64,
            false,
        )
        .expect("field");
        let baseline = profile(1, 1, schema(vec![field.clone()]), &[20, 0], 0);
        let below = profile(4, 2, schema(vec![field.clone()]), &[18, 2], 0);
        let equal = profile(2, 2, schema(vec![field.clone()]), &[16, 4], 0);
        let above = profile(3, 3, schema(vec![field]), &[0, 20], 0);

        let below_result = compare(DriftRequest {
            comparison: request(&below, &baseline),
            baseline: Some(baseline.clone()),
            candidate: below,
            context: stillflow_core::RequestContext::default(),
        })
        .expect("comparison");
        assert_eq!(below_result.outcome, DriftOutcome::Complete);
        assert!(below_result.report.expect("report").findings.is_empty());

        let equal_result = compare(DriftRequest {
            comparison: request(&equal, &baseline),
            baseline: Some(baseline.clone()),
            candidate: equal,
            context: stillflow_core::RequestContext::default(),
        })
        .expect("comparison");
        assert_eq!(equal_result.outcome, DriftOutcome::Complete);
        assert!(equal_result.report.expect("report").findings.is_empty());

        let above_result = compare(DriftRequest {
            comparison: request(&above, &baseline),
            baseline: Some(baseline),
            candidate: above,
            context: stillflow_core::RequestContext::default(),
        })
        .expect("comparison");
        let report = above_result.report.expect("report");
        assert_eq!(report.findings.len(), 1);
        assert_eq!(
            report.findings[0].kind,
            DriftFindingKind::DistributionNumericHistogramL1Exceeded
        );
        assert_eq!(report.findings[0].observed, DriftRational::new(1, 1));
    }

    #[test]
    fn null_rate_uses_exact_strict_threshold() {
        let field = LogicalField::new(
            ColumnId::from_uuid(Uuid::from_u128(12)),
            "value",
            LogicalType::Int64,
            false,
        )
        .expect("field");
        let baseline = profile_with_counts(4, 1, schema(vec![field.clone()]), &[20, 0], 40, 0, 40);
        let equal = profile_with_counts(5, 2, schema(vec![field.clone()]), &[20, 0], 40, 4, 36);
        let above = profile_with_counts(6, 3, schema(vec![field]), &[20, 0], 40, 8, 32);
        for (candidate, expected) in [(equal, 0_usize), (above, 1_usize)] {
            let result = compare(DriftRequest {
                comparison: request(&candidate, &baseline),
                baseline: Some(baseline.clone()),
                candidate,
                context: stillflow_core::RequestContext::default(),
            })
            .expect("comparison");
            let report = result.report.expect("report");
            assert_eq!(
                report
                    .findings
                    .iter()
                    .filter(|finding| {
                        finding.kind == DriftFindingKind::DistributionNullRateDeltaExceeded
                    })
                    .count(),
                expected
            );
        }
    }

    #[test]
    fn schema_findings_are_stable_and_nullability_is_independent() {
        let baseline_field = LogicalField::new(
            ColumnId::from_uuid(Uuid::from_u128(21)),
            "value",
            LogicalType::Int64,
            false,
        )
        .expect("field");
        let candidate_field = LogicalField::new(
            ColumnId::from_uuid(Uuid::from_u128(21)),
            "value",
            LogicalType::Utf8,
            true,
        )
        .expect("field");
        let added_field = LogicalField::new(
            ColumnId::from_uuid(Uuid::from_u128(22)),
            "added",
            LogicalType::Utf8,
            false,
        )
        .expect("field");
        let baseline = profile(11, 1, schema(vec![baseline_field]), &[20, 0], 0);
        let candidate = profile(
            12,
            2,
            schema(vec![candidate_field, added_field]),
            &[20, 0],
            0,
        );
        let result = compare(DriftRequest {
            comparison: request(&candidate, &baseline),
            baseline: Some(baseline),
            candidate,
            context: stillflow_core::RequestContext::default(),
        })
        .expect("comparison");
        let report = result.report.expect("report");
        assert_eq!(report.findings.len(), 3);
        assert_eq!(report.findings[0].kind, DriftFindingKind::SchemaColumnAdded);
        assert_eq!(
            report.findings[1].kind,
            DriftFindingKind::SchemaColumnTypeChanged
        );
        assert_eq!(
            report.findings[2].kind,
            DriftFindingKind::SchemaColumnNullabilityChanged
        );
    }

    #[test]
    fn tombstones_and_missing_baselines_publish_no_report() {
        let field = LogicalField::new(
            ColumnId::from_uuid(Uuid::from_u128(31)),
            "value",
            LogicalType::Int64,
            false,
        )
        .expect("field");
        let candidate = profile(31, 2, schema(vec![field.clone()]), &[20, 0], 0);
        let no_baseline = compare(DriftRequest {
            comparison: DriftComparisonRequest {
                workspace_id: candidate.entry.workspace_id,
                dataset_id: candidate.entry.dataset_id,
                candidate_history_id: candidate.entry.history_id,
                baseline: DriftBaselineMode::LatestEligible,
                threshold_policy_version: 1,
                observation_window: None,
                report_contract_version: 1,
            },
            baseline: None,
            candidate: candidate.clone(),
            context: stillflow_core::RequestContext::default(),
        })
        .expect("comparison");
        assert_eq!(no_baseline.outcome, DriftOutcome::NoBaseline);
        assert!(no_baseline.report.is_none());

        let mut tombstoned = profile(32, 1, schema(vec![field]), &[20, 0], 0);
        tombstoned.entry.state = ProfileHistoryState::Tombstoned;
        let tombstone = compare(DriftRequest {
            comparison: request(&candidate, &tombstoned),
            baseline: Some(tombstoned),
            candidate,
            context: stillflow_core::RequestContext::default(),
        })
        .expect("comparison");
        assert_eq!(tombstone.outcome, DriftOutcome::TombstonedInput);
        assert!(tombstone.report.is_none());
    }

    #[test]
    fn canonical_digest_excludes_run_and_history_timestamps() {
        let field = LogicalField::new(
            ColumnId::from_uuid(Uuid::from_u128(41)),
            "value",
            LogicalType::Int64,
            false,
        )
        .expect("field");
        let baseline = profile(41, 1, schema(vec![field.clone()]), &[20, 0], 0);
        let candidate = profile(42, 2, schema(vec![field]), &[0, 20], 0);
        let mut changed_baseline = baseline.clone();
        changed_baseline.entry.producing_run_id = Uuid::from_u128(4_100_001);
        changed_baseline.entry.created_at = Utc::now();
        let mut changed_candidate = candidate.clone();
        changed_candidate.entry.producing_run_id = Uuid::from_u128(4_200_001);
        changed_candidate.entry.created_at = Utc::now();

        let first = compare(DriftRequest {
            comparison: request(&candidate, &baseline),
            baseline: Some(baseline),
            candidate,
            context: stillflow_core::RequestContext::default(),
        })
        .expect("comparison");
        let second = compare(DriftRequest {
            comparison: request(&changed_candidate, &changed_baseline),
            baseline: Some(changed_baseline),
            candidate: changed_candidate,
            context: stillflow_core::RequestContext::default(),
        })
        .expect("comparison");
        assert_eq!(first.canonical_digest, second.canonical_digest);
        assert_eq!(first.canonical_body, second.canonical_body);
    }
}
