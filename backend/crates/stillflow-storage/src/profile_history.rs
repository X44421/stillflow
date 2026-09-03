//! Durable Q-D1 profile history references.
//!
//! The table stores only Dataset-owned metadata. Profile bytes remain owned by
//! the E5 ArtifactRef/body and are read through the existing committed-body
//! gate. Every mutating operation is one SQLite transaction.

use std::fmt::Write as _;

use chrono::{DateTime, SecondsFormat, Utc};
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use stillflow_core::{
    ArtifactKind, ControlPlaneEventType, DriftBaselineMode, DriftObservationWindow,
    EventStreamKind, LogicalSchema,
};

use crate::{
    acquire_activity, append_event_tx, compact_json, map_constraint, open_connection,
    validate_artifact_body, validate_safe_json, ActivityKind, ControlPlaneStore, EventDraft,
    StorageError,
};

pub const PROFILE_HISTORY_ACTIVE: &str = "active";
pub const PROFILE_HISTORY_TOMBSTONED: &str = "tombstoned";
const PROFILE_HISTORY_PROFILE_CONTRACT_VERSION: u16 = 1;
const PROFILE_HISTORY_POLICY_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProfileHistoryState {
    Active,
    Tombstoned,
}

impl ProfileHistoryState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Active => PROFILE_HISTORY_ACTIVE,
            Self::Tombstoned => PROFILE_HISTORY_TOMBSTONED,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfileHistoryDraft {
    pub history_id: Uuid,
    pub workspace_id: Uuid,
    pub dataset_id: Uuid,
    pub profile_artifact_id: Uuid,
    pub producing_run_id: Uuid,
    pub profile_digest: [u8; 32],
    pub profile_contract_version: u16,
    pub drift_contract_version: u16,
    pub profile_policy_version: u16,
    pub top_k: usize,
    pub histogram_buckets: usize,
    pub schema_fingerprint: [u8; 32],
    pub schema: LogicalSchema,
    pub row_count_scanned: u64,
    pub scanned_bytes: u64,
    pub truncated: bool,
    pub profile_sequence: u64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProfileHistoryEntry {
    pub history_id: Uuid,
    pub workspace_id: Uuid,
    pub dataset_id: Uuid,
    pub profile_artifact_id: Uuid,
    pub producing_run_id: Uuid,
    #[serde(with = "digest_hex")]
    pub profile_digest: [u8; 32],
    pub profile_contract_version: u16,
    pub drift_contract_version: u16,
    pub profile_policy_version: u16,
    pub top_k: usize,
    pub histogram_buckets: usize,
    #[serde(with = "digest_hex")]
    pub schema_fingerprint: [u8; 32],
    pub schema: LogicalSchema,
    pub row_count_scanned: u64,
    pub scanned_bytes: u64,
    pub truncated: bool,
    pub profile_sequence: u64,
    pub state: ProfileHistoryState,
    pub created_at: DateTime<Utc>,
    pub tombstoned_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProfileHistoryCursor {
    pub workspace_id: Uuid,
    pub dataset_id: Uuid,
    pub state: Option<ProfileHistoryState>,
    pub profile_sequence: u64,
    pub history_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfileHistoryPage {
    pub entries: Vec<ProfileHistoryEntry>,
    pub next: Option<ProfileHistoryCursor>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DriftReportDraft {
    pub comparison_key: [u8; 32],
    pub workspace_id: Uuid,
    pub dataset_id: Uuid,
    pub baseline_history_id: Uuid,
    pub candidate_history_id: Uuid,
    pub report_artifact_id: Uuid,
    pub producing_run_id: Uuid,
    pub report_digest: [u8; 32],
    pub metadata: Value,
    pub body: Vec<u8>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DriftComparisonRecord {
    pub comparison_key: [u8; 32],
    pub workspace_id: Uuid,
    pub dataset_id: Uuid,
    pub baseline_history_id: Uuid,
    pub candidate_history_id: Uuid,
    pub report_artifact_id: Uuid,
    pub producing_run_id: Uuid,
    pub report_digest: [u8; 32],
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DriftReportCursor {
    pub report_digest: [u8; 32],
    pub offset: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DriftReportPage {
    pub report_artifact_id: Uuid,
    pub report_digest: [u8; 32],
    pub findings: Vec<Value>,
    pub next: Option<DriftReportCursor>,
}

#[derive(Debug, Clone)]
struct HistoryIdentity {
    workspace_id: Uuid,
    dataset_id: Uuid,
    profile_artifact_id: Uuid,
    producing_run_id: Uuid,
    profile_digest: [u8; 32],
    profile_sequence: u64,
    state: ProfileHistoryState,
}

impl ControlPlaneStore {
    /// Reads the durable idempotency result for one resolved comparison key.
    pub fn get_drift_comparison(
        &self,
        comparison_key: [u8; 32],
    ) -> Result<DriftComparisonRecord, StorageError> {
        if comparison_key == [0; 32] {
            return Err(StorageError::InvalidDraft(
                "drift comparison key must not be zero",
            ));
        }
        let _activity = acquire_activity(&self.inner, ActivityKind::Reader)?;
        let connection = open_connection(&self.inner)?;
        drift_comparison_from_connection(&connection, comparison_key)
    }

    /// Atomically commits one canonical drift report, its E5 Artifact body and
    /// the resolved comparison identity. A replay returns the first committed
    /// result without creating another Artifact or Event.
    pub fn publish_drift_report(
        &self,
        draft: DriftReportDraft,
        event: EventDraft,
    ) -> Result<DriftComparisonRecord, StorageError> {
        validate_drift_report_draft(&draft, &event)?;
        let metadata_json = compact_json(&draft.metadata, "serialize drift report metadata")?;
        let artifact_kind_json = serde_json::to_string(&ArtifactKind::DriftReport)
            .map_err(|_| StorageError::Serialization("serialize drift report Artifact kind"))?;
        let _activity = acquire_activity(&self.inner, ActivityKind::Publisher)?;
        let mut connection = open_connection(&self.inner)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StorageError::database("begin drift report publication"))?;

        if let Some(existing) = drift_comparison_by_transaction(&transaction, draft.comparison_key)?
        {
            transaction
                .commit()
                .map_err(|_| StorageError::database("commit drift report replay"))?;
            return Ok(existing);
        }

        let (baseline, candidate) = ensure_history_pair(
            &transaction,
            draft.workspace_id,
            draft.dataset_id,
            draft.baseline_history_id,
            draft.candidate_history_id,
        )?;
        if baseline.state != ProfileHistoryState::Active
            || candidate.state != ProfileHistoryState::Active
        {
            return Err(StorageError::InvalidDraft(
                "drift report inputs must be active at publication",
            ));
        }
        ensure_profile_artifact(&transaction, &baseline)?;
        ensure_profile_artifact(&transaction, &candidate)?;
        validate_report_body_identity(&draft, &baseline, &candidate)?;

        let (run_workspace, run_job, run_state, run_started_at, operation_kind): (
            String,
            String,
            String,
            String,
            Option<String>,
        ) = transaction
            .query_row(
                "SELECT workspace_id, job_id, session_id, state, started_at_utc,
                        operation_kind
                 FROM cp_runs WHERE id = ?1",
                params![draft.producing_run_id.to_string()],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .optional()
            .map_err(|_| StorageError::database("read drift report Run"))?
            .ok_or(StorageError::NotFound(draft.producing_run_id))?;
        if run_workspace != draft.workspace_id.to_string()
            || run_state != "running"
            || operation_kind.is_some()
        {
            return Err(StorageError::InvalidDraft(
                "Q-D1 report publication requires a running generic E5 Run",
            ));
        }
        let run_started_at = parse_timestamp(&run_started_at, "Run start timestamp")?;
        if draft.created_at < run_started_at || event.occurred_at < draft.created_at {
            return Err(StorageError::InvalidTimestampOrder(
                "drift report publication",
            ));
        }
        if event.stream_kind != EventStreamKind::Run
            || event.stream_id != draft.producing_run_id
            || event.job_id != parse_uuid(&run_job)?
            || event.run_id != Some(draft.producing_run_id)
            || event.event_type != ControlPlaneEventType::ArtifactCommitted
        {
            return Err(StorageError::InvalidDraft(
                "invalid drift report ArtifactCommitted event",
            ));
        }
        let report_id = draft.report_artifact_id.to_string();
        let created_at = timestamp(&draft.created_at);
        let committed_at = timestamp(&event.occurred_at);
        transaction
            .execute(
                "INSERT INTO cp_artifact_refs
                 (id, workspace_id, run_id, artifact_kind, external_ref_kind, external_ref_id,
                  content_digest, metadata_json, state, created_at_utc, committed_at_utc,
                  tombstoned_at_utc)
                 VALUES (?1, ?2, ?3, ?4, 'artifact', ?5, ?6, ?7, 'committed', ?8, ?9, NULL)",
                params![
                    report_id,
                    draft.workspace_id.to_string(),
                    draft.producing_run_id.to_string(),
                    artifact_kind_json,
                    candidate.profile_artifact_id.to_string(),
                    digest_hex(&draft.report_digest),
                    metadata_json,
                    created_at,
                    committed_at,
                ],
            )
            .map_err(|error| map_constraint(error, draft.report_artifact_id))?;
        transaction
            .execute(
                "INSERT INTO cp_artifact_bodies
                 (artifact_id, artifact_kind, artifact_version, content_digest, body,
                  provenance_json, state, created_at_utc, committed_at_utc)
                 VALUES (?1, ?2, 1, ?3, ?4, ?5, 'committed', ?6, ?7)",
                params![
                    report_id,
                    serde_json::to_string(&ArtifactKind::DriftReport).map_err(|_| {
                        StorageError::Serialization("serialize drift report body kind")
                    })?,
                    digest_hex(&draft.report_digest),
                    draft.body,
                    metadata_json,
                    created_at,
                    committed_at,
                ],
            )
            .map_err(|error| map_constraint(error, draft.report_artifact_id))?;
        append_event_tx(&transaction, event)?;
        transaction
            .execute(
                "INSERT INTO qd1_drift_comparisons
                 (comparison_key, workspace_id, dataset_id, baseline_history_id,
                  candidate_history_id, report_artifact_id, producing_run_id,
                  report_digest, created_at_utc)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    digest_hex(&draft.comparison_key),
                    draft.workspace_id.to_string(),
                    draft.dataset_id.to_string(),
                    draft.baseline_history_id.to_string(),
                    draft.candidate_history_id.to_string(),
                    draft.report_artifact_id.to_string(),
                    draft.producing_run_id.to_string(),
                    digest_hex(&draft.report_digest),
                    created_at,
                ],
            )
            .map_err(|error| map_constraint(error, draft.report_artifact_id))?;
        let record = drift_comparison_from_transaction(&transaction, draft.comparison_key)?;
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit drift report publication"))?;
        Ok(record)
    }

    /// Reads only a bounded finding page from a committed `drift_report.v1`.
    /// The cursor is tied to the verified report digest and cannot be reused
    /// for another report.
    pub fn list_drift_report_findings(
        &self,
        report_artifact_id: Uuid,
        cursor: Option<DriftReportCursor>,
        limit: usize,
    ) -> Result<DriftReportPage, StorageError> {
        if report_artifact_id.is_nil()
            || limit == 0
            || limit > stillflow_core::DRIFT_MAX_REPORT_PAGE_SIZE
        {
            return Err(StorageError::InvalidDraft(
                "drift report page request is outside the authorized bound",
            ));
        }
        let report = self.get_artifact_body(report_artifact_id)?;
        if report.artifact_kind != ArtifactKind::DriftReport {
            return Err(StorageError::InvalidDraft(
                "Artifact is not a committed drift report",
            ));
        }
        let value: Value = serde_json::from_slice(&report.body)
            .map_err(|_| StorageError::Serialization("decode drift report page"))?;
        let findings = value
            .get("findings")
            .and_then(Value::as_array)
            .ok_or(StorageError::Serialization("drift report findings"))?;
        let offset = match cursor {
            None => 0,
            Some(value) if value.report_digest == report.content_digest => value.offset,
            Some(_) => {
                return Err(StorageError::InvalidDraft(
                    "drift report cursor is bound to another report",
                ));
            }
        };
        if offset > findings.len() {
            return Err(StorageError::InvalidDraft(
                "drift report cursor is outside the report",
            ));
        }
        let end = offset.saturating_add(limit).min(findings.len());
        let next = (end < findings.len()).then_some(DriftReportCursor {
            report_digest: report.content_digest,
            offset: end,
        });
        Ok(DriftReportPage {
            report_artifact_id,
            report_digest: report.content_digest,
            findings: findings[offset..end].to_vec(),
            next,
        })
    }

    /// Resolves the baseline without consulting wall-clock fields. Explicit
    /// selection returns the named entry (including a tombstone so the Engine
    /// can surface TOMBSTONED_INPUT); latest selection is active-only and is
    /// ordered by Dataset sequence, then history identity.
    pub fn select_profile_history_baseline(
        &self,
        workspace_id: Uuid,
        dataset_id: Uuid,
        candidate_history_id: Uuid,
        mode: DriftBaselineMode,
        window: Option<DriftObservationWindow>,
    ) -> Result<Option<ProfileHistoryEntry>, StorageError> {
        validate_scope(workspace_id, dataset_id, candidate_history_id)?;
        if window.is_some_and(|value| !value.validate()) {
            return Err(StorageError::InvalidDraft(
                "ProfileHistory observation window is invalid",
            ));
        }
        let candidate = self.get_profile_history(workspace_id, dataset_id, candidate_history_id)?;
        if let Some(window) = window {
            if candidate.profile_sequence < window.start_sequence
                || candidate.profile_sequence >= window.end_sequence
            {
                return Err(StorageError::InvalidDraft(
                    "candidate is outside the ProfileHistory observation window",
                ));
            }
        }
        match mode {
            DriftBaselineMode::Explicit(history_id) => {
                if history_id == candidate_history_id {
                    return Err(StorageError::InvalidDraft(
                        "ProfileHistory comparison cannot self-compare",
                    ));
                }
                let baseline = self.get_profile_history(workspace_id, dataset_id, history_id)?;
                if baseline.profile_sequence >= candidate.profile_sequence {
                    return Err(StorageError::InvalidDraft(
                        "explicit ProfileHistory baseline must be older than candidate",
                    ));
                }
                if let Some(window) = window {
                    if baseline.profile_sequence < window.start_sequence
                        || baseline.profile_sequence >= window.end_sequence
                    {
                        return Err(StorageError::InvalidDraft(
                            "baseline is outside the ProfileHistory observation window",
                        ));
                    }
                }
                Ok(Some(baseline))
            }
            DriftBaselineMode::LatestEligible => {
                let (window_start, window_end) = if let Some(window) = window {
                    (
                        Some(i64::try_from(window.start_sequence).map_err(|_| {
                            StorageError::ArithmeticOverflow("ProfileHistory window start")
                        })?),
                        Some(i64::try_from(window.end_sequence).map_err(|_| {
                            StorageError::ArithmeticOverflow("ProfileHistory window end")
                        })?),
                    )
                } else {
                    (None, None)
                };
                let _activity = acquire_activity(&self.inner, ActivityKind::Reader)?;
                let connection = open_connection(&self.inner)?;
                let mut statement = connection
                    .prepare(
                        "SELECT history_id, workspace_id, dataset_id, profile_artifact_id,
                                producing_run_id, profile_digest, profile_contract_version,
                                drift_contract_version, profile_policy_version, top_k,
                                histogram_buckets, schema_fingerprint, schema_json,
                                row_count_scanned, scanned_bytes, truncated, profile_sequence,
                                state, created_at_utc, tombstoned_at_utc
                         FROM qd1_profile_history
                         WHERE workspace_id = ?1 AND dataset_id = ?2 AND state = 'active'
                           AND row_count_scanned > 0 AND truncated = 0
                           AND profile_sequence < ?3
                           AND (?4 IS NULL OR profile_sequence >= ?4)
                           AND (?5 IS NULL OR profile_sequence < ?5)
                         ORDER BY profile_sequence DESC, history_id DESC",
                    )
                    .map_err(|_| {
                        StorageError::database("prepare latest ProfileHistory baseline")
                    })?;
                let rows = statement
                    .query_map(
                        params![
                            workspace_id.to_string(),
                            dataset_id.to_string(),
                            i64::try_from(candidate.profile_sequence).map_err(|_| {
                                StorageError::ArithmeticOverflow(
                                    "ProfileHistory candidate sequence",
                                )
                            })?,
                            window_start,
                            window_end,
                        ],
                        raw_profile_history_from_row,
                    )
                    .map_err(|_| StorageError::database("read latest ProfileHistory baselines"))?;
                for row in rows {
                    let raw = row.map_err(|_| {
                        StorageError::database("read latest ProfileHistory baseline row")
                    })?;
                    let baseline = profile_history_from_raw(raw)?;
                    if !profile_history_versions_match(&baseline, &candidate) {
                        continue;
                    }
                    let body = match self.get_artifact_body(baseline.profile_artifact_id) {
                        Ok(body) => body,
                        Err(error) if is_ineligible_latest_error(&error) => continue,
                        Err(error) => return Err(error),
                    };
                    if validate_profile_history_artifact(&baseline, &body).is_err() {
                        continue;
                    }
                    return Ok(Some(baseline));
                }
                Ok(None)
            }
        }
    }

    /// Records one committed Profile artifact as a Dataset-owned history
    /// entry. The supplied sequence is never replaced by a local clock or a
    /// random value; SQLite rejects a non-monotonic or duplicate sequence.
    pub fn record_profile_history(
        &self,
        draft: ProfileHistoryDraft,
    ) -> Result<ProfileHistoryEntry, StorageError> {
        self.record_profile_history_inner(draft, false)
    }

    /// Records a committed Profile artifact and allocates the next Dataset
    /// sequence inside the same transaction. This is the runtime bridge used
    /// after E5 terminal publication, so concurrent workers cannot race a
    /// read-then-insert sequence allocation.
    pub fn record_profile_history_next(
        &self,
        draft: ProfileHistoryDraft,
    ) -> Result<ProfileHistoryEntry, StorageError> {
        self.record_profile_history_inner(draft, true)
    }

    fn record_profile_history_inner(
        &self,
        mut draft: ProfileHistoryDraft,
        allocate_sequence: bool,
    ) -> Result<ProfileHistoryEntry, StorageError> {
        validate_draft(&draft, allocate_sequence)?;
        let body = self.get_artifact_body(draft.profile_artifact_id)?;
        if body.artifact_kind != ArtifactKind::ProfileReport
            || body.run_id != draft.producing_run_id
            || body.workspace_id != draft.workspace_id
            || body.content_digest != draft.profile_digest
        {
            return Err(StorageError::InvalidDraft(
                "ProfileHistory artifact identity does not match the committed body",
            ));
        }
        validate_profile_history_draft_artifact(&draft, &body)?;

        let mut connection = open_connection(&self.inner)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StorageError::database("begin ProfileHistory insert"))?;
        ensure_scope(&transaction, &draft)?;

        if let Some(existing) = profile_history_by_identity(
            &transaction,
            draft.workspace_id,
            draft.dataset_id,
            draft.profile_artifact_id,
            draft.producing_run_id,
        )? {
            transaction
                .commit()
                .map_err(|_| StorageError::database("commit ProfileHistory replay"))?;
            return Ok(existing);
        }

        let maximum: Option<i64> = transaction
            .query_row(
                "SELECT MAX(profile_sequence) FROM qd1_profile_history
                 WHERE workspace_id = ?1 AND dataset_id = ?2",
                params![draft.workspace_id.to_string(), draft.dataset_id.to_string()],
                |row| row.get(0),
            )
            .map_err(|_| StorageError::database("read ProfileHistory sequence"))?;
        let next = maximum
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(StorageError::ArithmeticOverflow("ProfileHistory sequence"))?;
        if allocate_sequence {
            draft.profile_sequence = u64::try_from(next)
                .map_err(|_| StorageError::ArithmeticOverflow("ProfileHistory sequence"))?;
        } else if i64::try_from(draft.profile_sequence).ok() != Some(next) {
            return Err(StorageError::InvalidDraft(
                "ProfileHistory sequence is not the next Dataset sequence",
            ));
        }
        let schema_json = serde_json::to_string(&draft.schema)
            .map_err(|_| StorageError::Serialization("serialize ProfileHistory schema"))?;
        transaction
            .execute(
                "INSERT INTO qd1_profile_history
                 (history_id, workspace_id, dataset_id, profile_artifact_id,
                  producing_run_id, profile_digest, profile_contract_version,
                  drift_contract_version, profile_policy_version, top_k,
                  histogram_buckets, schema_fingerprint, schema_json,
                  row_count_scanned, scanned_bytes, truncated, profile_sequence,
                  state, created_at_utc, tombstoned_at_utc)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11,
                         ?12, ?13, ?14, ?15, ?16, ?17, 'active', ?18, NULL)",
                params![
                    draft.history_id.to_string(),
                    draft.workspace_id.to_string(),
                    draft.dataset_id.to_string(),
                    draft.profile_artifact_id.to_string(),
                    draft.producing_run_id.to_string(),
                    digest_hex(&draft.profile_digest),
                    i64::from(draft.profile_contract_version),
                    i64::from(draft.drift_contract_version),
                    i64::from(draft.profile_policy_version),
                    i64::try_from(draft.top_k)
                        .map_err(|_| StorageError::ArithmeticOverflow("ProfileHistory top_k"))?,
                    i64::try_from(draft.histogram_buckets).map_err(|_| {
                        StorageError::ArithmeticOverflow("ProfileHistory histogram buckets")
                    })?,
                    digest_hex(&draft.schema_fingerprint),
                    schema_json,
                    i64::try_from(draft.row_count_scanned).map_err(|_| {
                        StorageError::ArithmeticOverflow("ProfileHistory row count")
                    })?,
                    i64::try_from(draft.scanned_bytes)
                        .map_err(|_| StorageError::ArithmeticOverflow("ProfileHistory bytes"))?,
                    if draft.truncated { 1_i64 } else { 0_i64 },
                    i64::try_from(draft.profile_sequence).map_err(|_| {
                        StorageError::ArithmeticOverflow("ProfileHistory sequence")
                    })?,
                    timestamp(&draft.created_at),
                ],
            )
            .map_err(|_| StorageError::database("insert ProfileHistory entry"))?;
        let entry = profile_history_from_transaction(&transaction, draft.history_id)?;
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit ProfileHistory insert"))?;
        Ok(entry)
    }

    pub fn get_profile_history(
        &self,
        workspace_id: Uuid,
        dataset_id: Uuid,
        history_id: Uuid,
    ) -> Result<ProfileHistoryEntry, StorageError> {
        validate_scope(workspace_id, dataset_id, history_id)?;
        let _activity = acquire_activity(&self.inner, ActivityKind::Reader)?;
        let connection = open_connection(&self.inner)?;
        let entry = profile_history_from_connection(&connection, history_id)?;
        if entry.workspace_id != workspace_id || entry.dataset_id != dataset_id {
            return Err(StorageError::NotFound(history_id));
        }
        Ok(entry)
    }

    pub fn list_profile_history(
        &self,
        workspace_id: Uuid,
        dataset_id: Uuid,
        state: Option<ProfileHistoryState>,
        cursor: Option<ProfileHistoryCursor>,
        limit: usize,
    ) -> Result<ProfileHistoryPage, StorageError> {
        if workspace_id.is_nil() || dataset_id.is_nil() {
            return Err(StorageError::InvalidDraft("ProfileHistory scope is nil"));
        }
        if limit == 0 || limit > stillflow_core::DRIFT_MAX_HISTORY_PAGE_SIZE {
            return Err(StorageError::InvalidDraft(
                "ProfileHistory page limit is outside the authorized bound",
            ));
        }
        if let Some(cursor) = cursor {
            if cursor.workspace_id != workspace_id
                || cursor.dataset_id != dataset_id
                || cursor.state != state
            {
                return Err(StorageError::InvalidDraft(
                    "ProfileHistory cursor is bound to another scope or filter",
                ));
            }
        }
        let _activity = acquire_activity(&self.inner, ActivityKind::Reader)?;
        let connection = open_connection(&self.inner)?;
        let state_text = state.map(ProfileHistoryState::as_str);
        let (after_sequence, after_id) = if let Some(value) = cursor {
            (
                i64::try_from(value.profile_sequence).map_err(|_| {
                    StorageError::ArithmeticOverflow("ProfileHistory cursor sequence")
                })?,
                value.history_id.to_string(),
            )
        } else {
            (i64::MAX, String::new())
        };
        let mut statement = connection
            .prepare(
                "SELECT history_id, workspace_id, dataset_id, profile_artifact_id,
                        producing_run_id, profile_digest, profile_contract_version,
                        drift_contract_version, profile_policy_version, top_k,
                        histogram_buckets, schema_fingerprint, schema_json,
                        row_count_scanned, scanned_bytes, truncated, profile_sequence,
                        state, created_at_utc, tombstoned_at_utc
                 FROM qd1_profile_history
                 WHERE workspace_id = ?1 AND dataset_id = ?2
                   AND (?3 IS NULL OR state = ?3)
                   AND (profile_sequence < ?4 OR
                        (profile_sequence = ?4 AND history_id < ?5))
                 ORDER BY profile_sequence DESC, history_id DESC
                 LIMIT ?6",
            )
            .map_err(|_| StorageError::database("prepare ProfileHistory page"))?;
        let rows = statement
            .query_map(
                params![
                    workspace_id.to_string(),
                    dataset_id.to_string(),
                    state_text,
                    after_sequence,
                    after_id,
                    i64::try_from(limit + 1)
                        .map_err(|_| StorageError::ArithmeticOverflow("ProfileHistory page"))?,
                ],
                raw_profile_history_from_row,
            )
            .map_err(|_| StorageError::database("read ProfileHistory page"))?;
        let mut entries = Vec::with_capacity(limit);
        for row in rows {
            let entry = profile_history_from_raw(
                row.map_err(|_| StorageError::database("decode ProfileHistory page"))?,
            )?;
            if entries.len() < limit {
                let encoded = serde_json::to_vec(&entry)
                    .map_err(|_| StorageError::Serialization("encode ProfileHistory page"))?;
                let total = entries
                    .iter()
                    .map(|value: &ProfileHistoryEntry| {
                        serde_json::to_vec(value)
                            .map(|bytes| bytes.len())
                            .unwrap_or(usize::MAX)
                    })
                    .sum::<usize>()
                    .saturating_add(encoded.len());
                if total > stillflow_core::DRIFT_MAX_HISTORY_REFERENCE_BYTES {
                    return Err(StorageError::InvalidDraft(
                        "ProfileHistory page exceeds the metadata byte bound",
                    ));
                }
            }
            entries.push(entry);
        }
        let next = if entries.len() > limit {
            entries.pop();
            entries.last().map(|entry| ProfileHistoryCursor {
                workspace_id,
                dataset_id,
                state,
                profile_sequence: entry.profile_sequence,
                history_id: entry.history_id,
            })
        } else {
            None
        };
        Ok(ProfileHistoryPage { entries, next })
    }

    /// Tombstoning is a logical, idempotent transition. It never deletes the
    /// E5 Artifact body and never resurrects an entry.
    pub fn tombstone_profile_history(
        &self,
        workspace_id: Uuid,
        dataset_id: Uuid,
        history_id: Uuid,
        tombstoned_at: DateTime<Utc>,
    ) -> Result<ProfileHistoryEntry, StorageError> {
        validate_scope(workspace_id, dataset_id, history_id)?;
        let _activity = acquire_activity(&self.inner, ActivityKind::Publisher)?;
        let mut connection = open_connection(&self.inner)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StorageError::database("begin ProfileHistory tombstone"))?;
        let current = profile_history_from_transaction(&transaction, history_id)?;
        if current.workspace_id != workspace_id || current.dataset_id != dataset_id {
            return Err(StorageError::NotFound(history_id));
        }
        if current.state == ProfileHistoryState::Tombstoned {
            transaction
                .commit()
                .map_err(|_| StorageError::database("commit ProfileHistory tombstone replay"))?;
            return Ok(current);
        }
        if tombstoned_at < current.created_at {
            return Err(StorageError::InvalidTimestampOrder(
                "ProfileHistory creation and tombstone",
            ));
        }
        transaction
            .execute(
                "UPDATE qd1_profile_history
                 SET state = 'tombstoned', tombstoned_at_utc = ?2
                 WHERE history_id = ?1 AND state = 'active'",
                params![history_id.to_string(), timestamp(&tombstoned_at)],
            )
            .map_err(|_| StorageError::database("tombstone ProfileHistory"))?;
        let entry = profile_history_from_transaction(&transaction, history_id)?;
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit ProfileHistory tombstone"))?;
        Ok(entry)
    }
}

fn validate_drift_report_draft(
    draft: &DriftReportDraft,
    event: &EventDraft,
) -> Result<(), StorageError> {
    if draft.workspace_id.is_nil()
        || draft.dataset_id.is_nil()
        || draft.baseline_history_id.is_nil()
        || draft.candidate_history_id.is_nil()
        || draft.report_artifact_id.is_nil()
        || draft.producing_run_id.is_nil()
        || draft.baseline_history_id == draft.candidate_history_id
        || draft.comparison_key == [0; 32]
        || draft.report_digest == [0; 32]
    {
        return Err(StorageError::InvalidDraft(
            "drift report identities and digests must be non-zero and distinct",
        ));
    }
    if draft.body.is_empty() || draft.body.len() > stillflow_core::DRIFT_MAX_REPORT_BYTES {
        return Err(StorageError::InvalidDraft(
            "drift report body exceeds the authorized bound",
        ));
    }
    validate_safe_json(&draft.metadata, false)?;
    validate_artifact_body(ArtifactKind::DriftReport, draft.report_digest, &draft.body)?;
    if event.stream_kind != EventStreamKind::Run
        || event.stream_id != draft.producing_run_id
        || event.run_id != Some(draft.producing_run_id)
        || event.event_type != ControlPlaneEventType::ArtifactCommitted
    {
        return Err(StorageError::InvalidDraft(
            "drift report publication requires a Run ArtifactCommitted event",
        ));
    }
    Ok(())
}

fn validate_report_body_identity(
    draft: &DriftReportDraft,
    baseline: &HistoryIdentity,
    candidate: &HistoryIdentity,
) -> Result<(), StorageError> {
    let value: Value = serde_json::from_slice(&draft.body)
        .map_err(|_| StorageError::Serialization("decode drift report body"))?;
    let key = digest_hex(&draft.comparison_key);
    let baseline_digest = digest_hex(&baseline.profile_digest);
    let candidate_digest = digest_hex(&candidate.profile_digest);
    if value.get("canonical_input_digest").and_then(Value::as_str) != Some(key.as_str())
        || value.get("baseline_profile_digest").and_then(Value::as_str)
            != Some(baseline_digest.as_str())
        || value
            .get("candidate_profile_digest")
            .and_then(Value::as_str)
            != Some(candidate_digest.as_str())
        || !matches!(
            value.get("outcome").and_then(Value::as_str),
            Some("complete") | Some("partial")
        )
    {
        return Err(StorageError::InvalidDraft(
            "drift report body does not match its resolved comparison",
        ));
    }
    Ok(())
}

fn ensure_history_pair(
    transaction: &rusqlite::Transaction<'_>,
    workspace_id: Uuid,
    dataset_id: Uuid,
    baseline_history_id: Uuid,
    candidate_history_id: Uuid,
) -> Result<(HistoryIdentity, HistoryIdentity), StorageError> {
    let baseline = history_identity(transaction, baseline_history_id)?;
    let candidate = history_identity(transaction, candidate_history_id)?;
    if baseline.workspace_id != workspace_id
        || candidate.workspace_id != workspace_id
        || baseline.dataset_id != dataset_id
        || candidate.dataset_id != dataset_id
        || baseline_history_id == candidate_history_id
        || baseline.profile_sequence >= candidate.profile_sequence
    {
        return Err(StorageError::InvalidDraft(
            "drift report histories are outside one ordered Dataset scope",
        ));
    }
    Ok((baseline, candidate))
}

fn history_identity(
    transaction: &rusqlite::Transaction<'_>,
    history_id: Uuid,
) -> Result<HistoryIdentity, StorageError> {
    let row: Option<(String, String, String, String, String, i64, String)> = transaction
        .query_row(
            "SELECT workspace_id, dataset_id, profile_artifact_id, producing_run_id,
                    profile_digest, profile_sequence, state
             FROM qd1_profile_history WHERE history_id = ?1",
            params![history_id.to_string()],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .optional()
        .map_err(|_| StorageError::database("read drift report history"))?;
    let Some((workspace, dataset, artifact, run, digest, sequence, state)) = row else {
        return Err(StorageError::NotFound(history_id));
    };
    let state = match state.as_str() {
        PROFILE_HISTORY_ACTIVE => ProfileHistoryState::Active,
        PROFILE_HISTORY_TOMBSTONED => ProfileHistoryState::Tombstoned,
        _ => return Err(StorageError::Serialization("drift report history state")),
    };
    Ok(HistoryIdentity {
        workspace_id: parse_uuid(&workspace)?,
        dataset_id: parse_uuid(&dataset)?,
        profile_artifact_id: parse_uuid(&artifact)?,
        producing_run_id: parse_uuid(&run)?,
        profile_digest: parse_digest(&digest)?,
        profile_sequence: positive_u64(sequence, "drift report profile sequence")?,
        state,
    })
}

#[allow(clippy::type_complexity)]
fn ensure_profile_artifact(
    transaction: &rusqlite::Transaction<'_>,
    history: &HistoryIdentity,
) -> Result<(), StorageError> {
    let row: Option<(
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        Vec<u8>,
    )> = transaction
        .query_row(
            "SELECT a.workspace_id, a.run_id, a.artifact_kind, a.state,
                        b.artifact_kind, b.state, b.content_digest, b.body
                 FROM cp_artifact_refs AS a
                 JOIN cp_artifact_bodies AS b ON b.artifact_id = a.id
                 WHERE a.id = ?1",
            params![history.profile_artifact_id.to_string()],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                ))
            },
        )
        .optional()
        .map_err(|_| StorageError::database("read drift report profile Artifact"))?;
    let Some((workspace, run, artifact_kind, artifact_state, body_kind, body_state, digest, body)) =
        row
    else {
        return Err(StorageError::NotFound(history.profile_artifact_id));
    };
    let expected_kind = serde_json::to_string(&ArtifactKind::ProfileReport)
        .map_err(|_| StorageError::Serialization("serialize profile Artifact kind"))?;
    if workspace != history.workspace_id.to_string()
        || run != history.producing_run_id.to_string()
        || artifact_kind != expected_kind
        || body_kind != expected_kind
        || artifact_state != "committed"
        || body_state != "committed"
        || Sha256::digest(&body).as_slice() != history.profile_digest
    {
        return Err(StorageError::InvalidDraft(
            "drift report input profile Artifact is not committed and immutable",
        ));
    }
    if parse_digest(&digest)? != history.profile_digest {
        return Err(StorageError::InvalidManifest(
            "drift report input profile digest mismatch",
        ));
    }
    Ok(())
}

fn validate_draft(
    draft: &ProfileHistoryDraft,
    allow_unassigned_sequence: bool,
) -> Result<(), StorageError> {
    validate_scope(draft.workspace_id, draft.dataset_id, draft.history_id)?;
    if draft.profile_artifact_id.is_nil() || draft.producing_run_id.is_nil() {
        return Err(StorageError::InvalidDraft(
            "ProfileHistory artifact and Run identities must not be nil",
        ));
    }
    if (!allow_unassigned_sequence && draft.profile_sequence == 0)
        || draft.profile_contract_version != PROFILE_HISTORY_PROFILE_CONTRACT_VERSION
        || draft.drift_contract_version != stillflow_core::PROFILE_HISTORY_DRIFT_CONTRACT_VERSION
        || draft.profile_policy_version != PROFILE_HISTORY_POLICY_VERSION
        || draft.top_k == 0
        || draft.histogram_buckets == 0
    {
        return Err(StorageError::InvalidDraft(
            "ProfileHistory versions, knobs, and sequence must be positive",
        ));
    }
    draft
        .schema
        .validate()
        .map_err(|_| StorageError::InvalidDraft("ProfileHistory schema is invalid"))?;
    let expected = stillflow_core::LogicalSchemaFingerprint::try_from_schema(&draft.schema)
        .map_err(|_| StorageError::InvalidDraft("ProfileHistory schema fingerprint failed"))?;
    if expected.as_bytes() != &draft.schema_fingerprint {
        return Err(StorageError::InvalidDraft(
            "ProfileHistory schema fingerprint does not match schema",
        ));
    }
    Ok(())
}

fn validate_scope(
    workspace_id: Uuid,
    dataset_id: Uuid,
    history_id: Uuid,
) -> Result<(), StorageError> {
    if workspace_id.is_nil() || dataset_id.is_nil() || history_id.is_nil() {
        return Err(StorageError::InvalidDraft("ProfileHistory identity is nil"));
    }
    Ok(())
}

fn ensure_scope(
    transaction: &rusqlite::Transaction<'_>,
    draft: &ProfileHistoryDraft,
) -> Result<(), StorageError> {
    let dataset_workspace: Option<String> = transaction
        .query_row(
            "SELECT workspace_id FROM cp_datasets WHERE id = ?1",
            params![draft.dataset_id.to_string()],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| StorageError::database("check ProfileHistory Dataset"))?;
    if dataset_workspace.as_deref() != Some(&draft.workspace_id.to_string()) {
        return Err(StorageError::InvalidDraft(
            "ProfileHistory Dataset is outside the Workspace",
        ));
    }
    let (artifact_workspace, artifact_run, artifact_kind, artifact_state, snapshot_id): (
        String,
        String,
        String,
        String,
        String,
    ) = transaction
        .query_row(
            "SELECT workspace_id, run_id, artifact_kind, state, external_ref_id
             FROM cp_artifact_refs WHERE id = ?1",
            params![draft.profile_artifact_id.to_string()],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .optional()
        .map_err(|_| StorageError::database("check ProfileHistory Artifact"))?
        .ok_or(StorageError::NotFound(draft.profile_artifact_id))?;
    if artifact_workspace != draft.workspace_id.to_string()
        || artifact_run != draft.producing_run_id.to_string()
        || artifact_kind
            != serde_json::to_string(&ArtifactKind::ProfileReport).map_err(|_| {
                StorageError::Serialization("serialize ProfileHistory Artifact kind")
            })?
        || artifact_state != "committed"
    {
        return Err(StorageError::InvalidDraft(
            "ProfileHistory requires a committed Profile artifact owned by the Run",
        ));
    }
    let snapshot_dataset: Option<String> = transaction
        .query_row(
            "SELECT dataset_id FROM snapshots WHERE id = ?1 AND state = 1",
            params![snapshot_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| StorageError::database("check ProfileHistory Snapshot"))?;
    if snapshot_dataset.as_deref() != Some(&draft.dataset_id.to_string()) {
        return Err(StorageError::InvalidDraft(
            "ProfileHistory artifact input is outside the Dataset",
        ));
    }
    Ok(())
}

type RawProfileHistory = (
    String,
    String,
    String,
    String,
    String,
    String,
    i64,
    i64,
    i64,
    i64,
    i64,
    String,
    String,
    i64,
    i64,
    i64,
    i64,
    String,
    String,
    Option<String>,
);

fn raw_profile_history_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawProfileHistory> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
        row.get(6)?,
        row.get(7)?,
        row.get(8)?,
        row.get(9)?,
        row.get(10)?,
        row.get(11)?,
        row.get(12)?,
        row.get(13)?,
        row.get(14)?,
        row.get(15)?,
        row.get(16)?,
        row.get(17)?,
        row.get(18)?,
        row.get(19)?,
    ))
}

fn profile_history_from_connection(
    connection: &rusqlite::Connection,
    history_id: Uuid,
) -> Result<ProfileHistoryEntry, StorageError> {
    let raw = connection
        .query_row(
            "SELECT history_id, workspace_id, dataset_id, profile_artifact_id,
                    producing_run_id, profile_digest, profile_contract_version,
                    drift_contract_version, profile_policy_version, top_k,
                    histogram_buckets, schema_fingerprint, schema_json,
                    row_count_scanned, scanned_bytes, truncated, profile_sequence,
                    state, created_at_utc, tombstoned_at_utc
             FROM qd1_profile_history WHERE history_id = ?1",
            params![history_id.to_string()],
            raw_profile_history_from_row,
        )
        .optional()
        .map_err(|_| StorageError::database("read ProfileHistory entry"))?
        .ok_or(StorageError::NotFound(history_id))?;
    profile_history_from_raw(raw)
}

fn profile_history_from_transaction(
    transaction: &rusqlite::Transaction<'_>,
    history_id: Uuid,
) -> Result<ProfileHistoryEntry, StorageError> {
    let raw = transaction
        .query_row(
            "SELECT history_id, workspace_id, dataset_id, profile_artifact_id,
                    producing_run_id, profile_digest, profile_contract_version,
                    drift_contract_version, profile_policy_version, top_k,
                    histogram_buckets, schema_fingerprint, schema_json,
                    row_count_scanned, scanned_bytes, truncated, profile_sequence,
                    state, created_at_utc, tombstoned_at_utc
             FROM qd1_profile_history WHERE history_id = ?1",
            params![history_id.to_string()],
            raw_profile_history_from_row,
        )
        .optional()
        .map_err(|_| StorageError::database("read ProfileHistory transaction entry"))?
        .ok_or(StorageError::NotFound(history_id))?;
    profile_history_from_raw(raw)
}

fn profile_history_by_identity(
    transaction: &rusqlite::Transaction<'_>,
    workspace_id: Uuid,
    dataset_id: Uuid,
    artifact_id: Uuid,
    run_id: Uuid,
) -> Result<Option<ProfileHistoryEntry>, StorageError> {
    let history_id: Option<String> = transaction
        .query_row(
            "SELECT history_id FROM qd1_profile_history
             WHERE workspace_id = ?1 AND dataset_id = ?2
               AND profile_artifact_id = ?3 AND producing_run_id = ?4",
            params![
                workspace_id.to_string(),
                dataset_id.to_string(),
                artifact_id.to_string(),
                run_id.to_string()
            ],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| StorageError::database("read ProfileHistory idempotency"))?;
    history_id
        .map(|value| {
            let id = Uuid::parse_str(&value)
                .map_err(|_| StorageError::Serialization("ProfileHistory identity"))?;
            profile_history_from_transaction(transaction, id)
        })
        .transpose()
}

fn drift_comparison_by_transaction(
    transaction: &rusqlite::Transaction<'_>,
    comparison_key: [u8; 32],
) -> Result<Option<DriftComparisonRecord>, StorageError> {
    let row = transaction
        .query_row(
            "SELECT comparison_key, workspace_id, dataset_id, baseline_history_id,
                    candidate_history_id, report_artifact_id, producing_run_id,
                    report_digest, created_at_utc
             FROM qd1_drift_comparisons WHERE comparison_key = ?1",
            params![digest_hex(&comparison_key)],
            raw_drift_comparison_from_row,
        )
        .optional()
        .map_err(|_| StorageError::database("read drift comparison idempotency"))?;
    row.map(drift_comparison_from_raw).transpose()
}

fn drift_comparison_from_connection(
    connection: &rusqlite::Connection,
    comparison_key: [u8; 32],
) -> Result<DriftComparisonRecord, StorageError> {
    let raw = connection
        .query_row(
            "SELECT comparison_key, workspace_id, dataset_id, baseline_history_id,
                    candidate_history_id, report_artifact_id, producing_run_id,
                    report_digest, created_at_utc
             FROM qd1_drift_comparisons WHERE comparison_key = ?1",
            params![digest_hex(&comparison_key)],
            raw_drift_comparison_from_row,
        )
        .optional()
        .map_err(|_| StorageError::database("read drift comparison"))?
        .ok_or(StorageError::InvalidDraft("drift comparison was not found"));
    raw.and_then(drift_comparison_from_raw)
}

fn drift_comparison_from_transaction(
    transaction: &rusqlite::Transaction<'_>,
    comparison_key: [u8; 32],
) -> Result<DriftComparisonRecord, StorageError> {
    let raw = transaction
        .query_row(
            "SELECT comparison_key, workspace_id, dataset_id, baseline_history_id,
                    candidate_history_id, report_artifact_id, producing_run_id,
                    report_digest, created_at_utc
             FROM qd1_drift_comparisons WHERE comparison_key = ?1",
            params![digest_hex(&comparison_key)],
            raw_drift_comparison_from_row,
        )
        .map_err(|_| StorageError::database("read committed drift comparison"))?;
    drift_comparison_from_raw(raw)
}

type RawDriftComparison = (
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
);

fn raw_drift_comparison_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawDriftComparison> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
        row.get(6)?,
        row.get(7)?,
        row.get(8)?,
    ))
}

fn drift_comparison_from_raw(
    raw: RawDriftComparison,
) -> Result<DriftComparisonRecord, StorageError> {
    Ok(DriftComparisonRecord {
        comparison_key: parse_digest(&raw.0)?,
        workspace_id: parse_uuid(&raw.1)?,
        dataset_id: parse_uuid(&raw.2)?,
        baseline_history_id: parse_uuid(&raw.3)?,
        candidate_history_id: parse_uuid(&raw.4)?,
        report_artifact_id: parse_uuid(&raw.5)?,
        producing_run_id: parse_uuid(&raw.6)?,
        report_digest: parse_digest(&raw.7)?,
        created_at: parse_timestamp(&raw.8, "drift comparison timestamp")?,
    })
}

fn profile_history_from_raw(raw: RawProfileHistory) -> Result<ProfileHistoryEntry, StorageError> {
    let state = match raw.17.as_str() {
        PROFILE_HISTORY_ACTIVE => ProfileHistoryState::Active,
        PROFILE_HISTORY_TOMBSTONED => ProfileHistoryState::Tombstoned,
        _ => return Err(StorageError::Serialization("ProfileHistory state")),
    };
    let schema: LogicalSchema = serde_json::from_str(&raw.12)
        .map_err(|_| StorageError::Serialization("ProfileHistory schema"))?;
    let profile_digest = parse_digest(&raw.5)?;
    let schema_fingerprint = parse_digest(&raw.11)?;
    Ok(ProfileHistoryEntry {
        history_id: parse_uuid(&raw.0)?,
        workspace_id: parse_uuid(&raw.1)?,
        dataset_id: parse_uuid(&raw.2)?,
        profile_artifact_id: parse_uuid(&raw.3)?,
        producing_run_id: parse_uuid(&raw.4)?,
        profile_digest,
        profile_contract_version: positive_u16(raw.6, "ProfileHistory profile version")?,
        drift_contract_version: positive_u16(raw.7, "ProfileHistory drift version")?,
        profile_policy_version: positive_u16(raw.8, "ProfileHistory policy version")?,
        top_k: positive_usize(raw.9, "ProfileHistory top_k")?,
        histogram_buckets: positive_usize(raw.10, "ProfileHistory histogram buckets")?,
        schema_fingerprint,
        schema,
        row_count_scanned: nonnegative_u64(raw.13, "ProfileHistory row count")?,
        scanned_bytes: positive_or_zero_u64(raw.14, "ProfileHistory scanned bytes")?,
        truncated: raw.15 != 0,
        profile_sequence: positive_u64(raw.16, "ProfileHistory sequence")?,
        state,
        created_at: parse_timestamp(&raw.18, "ProfileHistory creation timestamp")?,
        tombstoned_at: raw
            .19
            .as_deref()
            .map(|value| parse_timestamp(value, "ProfileHistory tombstone timestamp"))
            .transpose()?,
    })
}

#[allow(clippy::too_many_arguments)]
fn validate_profile_body(
    body: &[u8],
    expected_digest: [u8; 32],
    expected_profile_version: u16,
    expected_policy_version: u16,
    expected_top_k: usize,
    expected_histogram_buckets: usize,
    expected_schema_fingerprint: [u8; 32],
    expected_schema: &LogicalSchema,
    expected_row_count: u64,
    expected_scanned_bytes: u64,
    expected_truncated: bool,
    artifact_provenance: &Value,
) -> Result<(), StorageError> {
    let digest: [u8; 32] = Sha256::digest(body).into();
    if digest != expected_digest {
        return Err(StorageError::InvalidManifest(
            "ProfileHistory profile body digest mismatch",
        ));
    }
    let value: Value = serde_json::from_slice(body)
        .map_err(|_| StorageError::Serialization("ProfileHistory profile body"))?;
    if value.get("artifact_type").and_then(Value::as_str) != Some("profile_report")
        || value.get("artifact_body_version").and_then(Value::as_u64) != Some(1)
        || value
            .get("profiling_contract_version")
            .and_then(Value::as_u64)
            != Some(u64::from(expected_profile_version))
    {
        return Err(StorageError::InvalidDraft(
            "ProfileHistory body is not a compatible profile_report.v1",
        ));
    }
    let dataset =
        value
            .get("dataset")
            .and_then(Value::as_object)
            .ok_or(StorageError::InvalidDraft(
                "ProfileHistory body dataset metrics are missing",
            ))?;
    if dataset.get("row_count_scanned").and_then(Value::as_u64) != Some(expected_row_count)
        || dataset.get("truncated").and_then(Value::as_bool) != Some(expected_truncated)
    {
        return Err(StorageError::InvalidDraft(
            "ProfileHistory body scan scope does not match persisted history",
        ));
    }
    let columns =
        value
            .get("columns")
            .and_then(Value::as_array)
            .ok_or(StorageError::InvalidDraft(
                "ProfileHistory body columns are missing",
            ))?;
    if dataset.get("column_count_profiled").and_then(Value::as_u64) != Some(columns.len() as u64) {
        return Err(StorageError::InvalidDraft(
            "ProfileHistory body column count is inconsistent",
        ));
    }
    let selected_columns = columns
        .iter()
        .map(|column| {
            column
                .get("name")
                .and_then(Value::as_str)
                .ok_or(StorageError::InvalidDraft(
                    "ProfileHistory body column name is missing",
                ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if selected_columns.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(StorageError::InvalidDraft(
            "ProfileHistory body contains duplicate columns",
        ));
    }
    for column in columns {
        let column_name =
            column
                .get("name")
                .and_then(Value::as_str)
                .ok_or(StorageError::InvalidDraft(
                    "ProfileHistory body column name is missing",
                ))?;
        let schema_field = expected_schema
            .fields
            .iter()
            .find(|field| field.name == column_name)
            .ok_or(StorageError::InvalidDraft(
                "ProfileHistory body column is outside the persisted schema",
            ))?;
        if column.get("type").and_then(Value::as_str)
            != Some(profile_type_name(&schema_field.data_type))
        {
            return Err(StorageError::InvalidDraft(
                "ProfileHistory body column type does not match the persisted schema",
            ));
        }
        if let Some(histogram) = column.get("histogram") {
            let counts = histogram
                .get("counts")
                .or(Some(histogram))
                .and_then(Value::as_array)
                .ok_or(StorageError::InvalidDraft(
                    "ProfileHistory histogram counts are invalid",
                ))?;
            if counts.len() != expected_histogram_buckets {
                return Err(StorageError::InvalidDraft(
                    "ProfileHistory histogram bucket count does not match policy",
                ));
            }
        }
        if let Some(top_values) = column.get("top_values").and_then(Value::as_array) {
            if top_values.len() > expected_top_k {
                return Err(StorageError::InvalidDraft(
                    "ProfileHistory top values exceed policy",
                ));
            }
        }
    }
    let metadata = artifact_provenance
        .as_object()
        .ok_or(StorageError::InvalidDraft(
            "ProfileHistory artifact provenance is missing",
        ))?;
    if parse_digest(
        metadata
            .get("canonicalDigest")
            .and_then(Value::as_str)
            .ok_or(StorageError::InvalidDraft(
                "ProfileHistory canonical digest provenance is missing",
            ))?,
    )? != expected_digest
    {
        return Err(StorageError::InvalidDraft(
            "ProfileHistory canonical digest provenance does not match body",
        ));
    }
    let provenance = metadata
        .get("provenance")
        .and_then(Value::as_object)
        .ok_or(StorageError::InvalidDraft(
            "ProfileHistory scan provenance is missing",
        ))?;
    if provenance
        .get("profilingContractVersion")
        .and_then(Value::as_u64)
        != Some(u64::from(expected_profile_version))
        || provenance
            .get("profilePolicyVersion")
            .and_then(Value::as_u64)
            != Some(u64::from(expected_policy_version))
        || provenance.get("topK").and_then(Value::as_u64) != Some(expected_top_k as u64)
        || provenance.get("histogramBuckets").and_then(Value::as_u64)
            != Some(expected_histogram_buckets as u64)
        || parse_digest(
            provenance
                .get("schemaFingerprint")
                .and_then(Value::as_str)
                .ok_or(StorageError::InvalidDraft(
                    "ProfileHistory schema fingerprint provenance is missing",
                ))?,
        )? != expected_schema_fingerprint
    {
        return Err(StorageError::InvalidDraft(
            "ProfileHistory policy or schema provenance does not match history",
        ));
    }
    let provenance_columns = provenance
        .get("selectedColumns")
        .and_then(Value::as_array)
        .ok_or(StorageError::InvalidDraft(
            "ProfileHistory selected column provenance is missing",
        ))?
        .iter()
        .map(|column| {
            column.as_str().ok_or(StorageError::InvalidDraft(
                "ProfileHistory selected column provenance is invalid",
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if provenance_columns != selected_columns {
        return Err(StorageError::InvalidDraft(
            "ProfileHistory selected column provenance does not match body",
        ));
    }
    let scan =
        provenance
            .get("scan")
            .and_then(Value::as_object)
            .ok_or(StorageError::InvalidDraft(
                "ProfileHistory scan provenance is missing",
            ))?;
    if scan.get("rowCountScanned").and_then(Value::as_u64) != Some(expected_row_count)
        || scan.get("scannedBytes").and_then(Value::as_u64) != Some(expected_scanned_bytes)
        || scan.get("truncated").and_then(Value::as_bool) != Some(expected_truncated)
    {
        return Err(StorageError::InvalidDraft(
            "ProfileHistory scan provenance does not match history",
        ));
    }
    for name in selected_columns {
        if !expected_schema
            .fields
            .iter()
            .any(|field| field.name == name)
        {
            return Err(StorageError::InvalidDraft(
                "ProfileHistory body column is outside the persisted schema",
            ));
        }
    }
    Ok(())
}

fn profile_type_name(data_type: &stillflow_core::LogicalType) -> &'static str {
    use stillflow_core::LogicalType;
    match data_type {
        LogicalType::Null => "null",
        LogicalType::Boolean => "boolean",
        LogicalType::Int8 => "int8",
        LogicalType::Int16 => "int16",
        LogicalType::Int32 => "int32",
        LogicalType::Int64 => "int64",
        LogicalType::UInt8 => "uint8",
        LogicalType::UInt16 => "uint16",
        LogicalType::UInt32 => "uint32",
        LogicalType::UInt64 => "uint64",
        LogicalType::Float32 => "float32",
        LogicalType::Float64 => "float64",
        LogicalType::Utf8 => "utf8",
        LogicalType::Binary => "binary",
        LogicalType::Date32 => "date32",
        LogicalType::Timestamp { unit, .. } => match unit {
            stillflow_core::TimeUnit::Second => "timestamp_s",
            stillflow_core::TimeUnit::Millisecond => "timestamp_ms",
            stillflow_core::TimeUnit::Microsecond => "timestamp_us",
            stillflow_core::TimeUnit::Nanosecond => "timestamp_ns",
        },
        LogicalType::List(_) => "list",
        LogicalType::Struct(_) => "struct",
    }
}

fn validate_profile_history_draft_artifact(
    draft: &ProfileHistoryDraft,
    body: &crate::ArtifactBodyRecord,
) -> Result<(), StorageError> {
    if body.artifact_id != draft.profile_artifact_id
        || body.artifact_kind != ArtifactKind::ProfileReport
        || body.run_id != draft.producing_run_id
        || body.workspace_id != draft.workspace_id
        || body.content_digest != draft.profile_digest
    {
        return Err(StorageError::InvalidDraft(
            "ProfileHistory artifact identity does not match the committed body",
        ));
    }
    validate_profile_body(
        &body.body,
        draft.profile_digest,
        draft.profile_contract_version,
        draft.profile_policy_version,
        draft.top_k,
        draft.histogram_buckets,
        draft.schema_fingerprint,
        &draft.schema,
        draft.row_count_scanned,
        draft.scanned_bytes,
        draft.truncated,
        &body.provenance,
    )
}

fn validate_profile_history_artifact(
    history: &ProfileHistoryEntry,
    body: &crate::ArtifactBodyRecord,
) -> Result<(), StorageError> {
    if body.artifact_id != history.profile_artifact_id
        || body.artifact_kind != ArtifactKind::ProfileReport
        || body.run_id != history.producing_run_id
        || body.workspace_id != history.workspace_id
        || body.content_digest != history.profile_digest
    {
        return Err(StorageError::InvalidDraft(
            "ProfileHistory artifact identity does not match history",
        ));
    }
    validate_profile_body(
        &body.body,
        history.profile_digest,
        history.profile_contract_version,
        history.profile_policy_version,
        history.top_k,
        history.histogram_buckets,
        history.schema_fingerprint,
        &history.schema,
        history.row_count_scanned,
        history.scanned_bytes,
        history.truncated,
        &body.provenance,
    )
}

fn profile_history_versions_match(
    baseline: &ProfileHistoryEntry,
    candidate: &ProfileHistoryEntry,
) -> bool {
    baseline.profile_contract_version == candidate.profile_contract_version
        && baseline.drift_contract_version == candidate.drift_contract_version
        && baseline.profile_policy_version == candidate.profile_policy_version
        && baseline.top_k == candidate.top_k
        && baseline.histogram_buckets == candidate.histogram_buckets
        && baseline.profile_contract_version == PROFILE_HISTORY_PROFILE_CONTRACT_VERSION
        && baseline.drift_contract_version == stillflow_core::PROFILE_HISTORY_DRIFT_CONTRACT_VERSION
        && baseline.profile_policy_version == PROFILE_HISTORY_POLICY_VERSION
}

fn is_ineligible_latest_error(error: &StorageError) -> bool {
    matches!(
        error,
        StorageError::NotFound(_)
            | StorageError::InvalidDraft(_)
            | StorageError::InvalidManifest(_)
            | StorageError::Serialization(_)
    )
}

fn parse_uuid(value: &str) -> Result<Uuid, StorageError> {
    Uuid::parse_str(value).map_err(|_| StorageError::Serialization("ProfileHistory UUID"))
}

fn parse_digest(value: &str) -> Result<[u8; 32], StorageError> {
    if value.len() != 64 {
        return Err(StorageError::Serialization("ProfileHistory digest"));
    }
    let mut digest = [0_u8; 32];
    for (index, byte) in digest.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| StorageError::Serialization("ProfileHistory digest"))?;
    }
    Ok(digest)
}

fn digest_hex(value: &[u8; 32]) -> String {
    let mut output = String::with_capacity(64);
    for byte in value {
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn timestamp(value: &DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Nanos, true)
}

fn parse_timestamp(value: &str, label: &'static str) -> Result<DateTime<Utc>, StorageError> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| StorageError::Serialization(label))
}

#[cfg(test)]
mod tests {
    use super::*;
    use stillflow_core::{ColumnId, LogicalField, LogicalSchemaFingerprint, LogicalType};

    fn profile_fixture() -> (Vec<u8>, [u8; 32], LogicalSchema, [u8; 32], Value) {
        let schema = LogicalSchema::new(vec![LogicalField::new(
            ColumnId::from_uuid(Uuid::from_u128(1)),
            "amount",
            LogicalType::Int64,
            false,
        )
        .expect("field")])
        .expect("schema");
        let schema_fingerprint = *LogicalSchemaFingerprint::try_from_schema(&schema)
            .expect("schema fingerprint")
            .as_bytes();
        let body = serde_json::to_vec(&serde_json::json!({
            "artifact_body_version": 1,
            "artifact_type": "profile_report",
            "columns": [{
                "name": "amount",
                "status": "profiled",
                "type": "int64"
            }],
            "dataset": {
                "column_count_profiled": 1,
                "row_count_scanned": 4,
                "truncated": false
            },
            "profiling_contract_version": 1
        }))
        .expect("body");
        let digest: [u8; 32] = Sha256::digest(&body).into();
        let metadata = serde_json::json!({
            "canonicalDigest": digest_hex(&digest),
            "provenance": {
                "profilingContractVersion": 1,
                "profilePolicyVersion": 1,
                "topK": 5,
                "histogramBuckets": 2,
                "schemaFingerprint": digest_hex(&schema_fingerprint),
                "selectedColumns": ["amount"],
                "scan": {
                    "rowCountScanned": 4,
                    "scannedBytes": 64,
                    "truncated": false
                }
            }
        });
        (body, digest, schema, schema_fingerprint, metadata)
    }

    #[test]
    fn profile_body_validation_binds_scan_policy_schema_and_digest() {
        let (body, digest, schema, schema_fingerprint, metadata) = profile_fixture();
        assert!(validate_profile_body(
            &body,
            digest,
            1,
            1,
            5,
            2,
            schema_fingerprint,
            &schema,
            4,
            64,
            false,
            &metadata,
        )
        .is_ok());

        let mut mismatched = metadata;
        mismatched["provenance"]["topK"] = Value::from(6_u64);
        assert!(validate_profile_body(
            &body,
            digest,
            1,
            1,
            5,
            2,
            schema_fingerprint,
            &schema,
            4,
            64,
            false,
            &mismatched,
        )
        .is_err());
    }

    #[test]
    fn latest_eligibility_requires_matching_version_tuple() {
        let base = ProfileHistoryEntry {
            history_id: Uuid::from_u128(1),
            workspace_id: Uuid::from_u128(2),
            dataset_id: Uuid::from_u128(3),
            profile_artifact_id: Uuid::from_u128(4),
            producing_run_id: Uuid::from_u128(5),
            profile_digest: [1; 32],
            profile_contract_version: 1,
            drift_contract_version: 1,
            profile_policy_version: 1,
            top_k: 5,
            histogram_buckets: 2,
            schema_fingerprint: [2; 32],
            schema: LogicalSchema::empty(),
            row_count_scanned: 1,
            scanned_bytes: 1,
            truncated: false,
            profile_sequence: 1,
            state: ProfileHistoryState::Active,
            created_at: Utc::now(),
            tombstoned_at: None,
        };
        let mut incompatible = base.clone();
        incompatible.top_k = 6;
        assert!(!profile_history_versions_match(&base, &incompatible));
        assert!(profile_history_versions_match(&base, &base));
    }
}

fn positive_u16(value: i64, label: &'static str) -> Result<u16, StorageError> {
    u16::try_from(value).map_err(|_| StorageError::Serialization(label))
}

fn positive_usize(value: i64, label: &'static str) -> Result<usize, StorageError> {
    usize::try_from(value).map_err(|_| StorageError::Serialization(label))
}

fn positive_u64(value: i64, label: &'static str) -> Result<u64, StorageError> {
    u64::try_from(value).map_err(|_| StorageError::Serialization(label))
}

fn nonnegative_u64(value: i64, label: &'static str) -> Result<u64, StorageError> {
    u64::try_from(value).map_err(|_| StorageError::Serialization(label))
}

fn positive_or_zero_u64(value: i64, label: &'static str) -> Result<u64, StorageError> {
    u64::try_from(value).map_err(|_| StorageError::Serialization(label))
}

mod digest_hex {
    use std::fmt::Write as _;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(value: &[u8; 32], serializer: S) -> Result<S::Ok, S::Error> {
        let mut output = String::with_capacity(64);
        for byte in value {
            let _ = write!(output, "{byte:02x}");
        }
        serializer.serialize_str(&output)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<[u8; 32], D::Error> {
        let value = String::deserialize(deserializer)?;
        if value.len() != 64 {
            return Err(serde::de::Error::custom("digest must be 64 hex characters"));
        }
        let mut result = [0_u8; 32];
        for (index, byte) in result.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
                .map_err(|_| serde::de::Error::custom("digest contains invalid hex"))?;
        }
        Ok(result)
    }
}
