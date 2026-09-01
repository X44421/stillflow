//! Transactional persistence for the E5 unified control plane.
//!
//! The module is deliberately a persistence adapter: it owns SQLite rows,
//! compare-and-set transitions, durable event sequencing, and bounded reads.
//! It does not start workers, schedule jobs, expose HTTP, or infer lifecycle
//! state from files.

use std::fmt;
use std::fmt::Write as _;
use std::path::Path;
use std::sync::Arc;

use chrono::{DateTime, SecondsFormat, Utc};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use stillflow_core::{
    ArtifactKind, ArtifactRefState, AssetKind, ConnectorKind, ControlPlaneEventType,
    ControlPlaneInput, CredentialRef, DatasetState, EventStreamKind, InputRef, JobState, PlanState,
    PlanVersionState, RunState, SessionState, SourceAssetState, SourceConnectionState,
    WorkspaceState, MAX_EVENT_PAGE_SIZE, MAX_EVENT_PAYLOAD_BYTES, MAX_QUEUED_JOBS_PER_WORKSPACE,
};

use crate::{
    acquire_activity, open_connection, ActivityKind, SnapshotStore, StorageError, StoreInner,
};

const EVENT_VERSION: u16 = 1;
const OPERATION_JOB_SUBMIT: &str = "job.submit";

#[derive(Clone)]
pub struct ControlPlaneStore {
    inner: Arc<StoreInner>,
}

impl fmt::Debug for ControlPlaneStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlPlaneStore")
            .field("storage_schema_version", &crate::STORAGE_SCHEMA_VERSION)
            .finish_non_exhaustive()
    }
}

impl ControlPlaneStore {
    /// Opens a managed root and upgrades it to the current storage schema.
    pub fn open(root: impl AsRef<Path>) -> Result<Self, StorageError> {
        let snapshot_store = SnapshotStore::open(root, crate::StorageLimits::default())?;
        Ok(Self {
            inner: Arc::clone(&snapshot_store.inner),
        })
    }

    /// Creates a control-plane view sharing the existing managed-root lock.
    pub fn from_snapshot_store(snapshot_store: &SnapshotStore) -> Self {
        Self {
            inner: Arc::clone(&snapshot_store.inner),
        }
    }

    pub fn schema_version(&self) -> u16 {
        crate::STORAGE_SCHEMA_VERSION
    }

    pub fn create_workspace(
        &self,
        workspace_id: Uuid,
        created_at: DateTime<Utc>,
    ) -> Result<WorkspaceRecord, StorageError> {
        validate_id(workspace_id, "workspace")?;
        let _activity = self.write_activity()?;
        let connection = open_connection(&self.inner)?;
        connection
            .execute(
                "INSERT INTO cp_workspaces (id, state, created_at_utc, archived_at_utc)
                 VALUES (?1, 'active', ?2, NULL)",
                params![workspace_id.to_string(), timestamp(&created_at)],
            )
            .map_err(|error| map_constraint(error, workspace_id))?;
        self.workspace_from_connection(&connection, workspace_id)
    }

    pub fn archive_workspace(
        &self,
        workspace_id: Uuid,
        archived_at: DateTime<Utc>,
    ) -> Result<WorkspaceRecord, StorageError> {
        let _activity = self.write_activity()?;
        let connection = open_connection(&self.inner)?;
        let created_at = self
            .workspace_from_connection(&connection, workspace_id)?
            .created_at;
        if archived_at < created_at {
            return Err(StorageError::InvalidTimestampOrder("Workspace archive"));
        }
        let changed = connection
            .execute(
                "UPDATE cp_workspaces
                 SET state = 'archived', archived_at_utc = ?2
                 WHERE id = ?1 AND state = 'active'",
                params![workspace_id.to_string(), timestamp(&archived_at)],
            )
            .map_err(|_| StorageError::database("archive workspace"))?;
        if changed != 1 {
            return Err(StorageError::NotFound(workspace_id));
        }
        self.workspace_from_connection(&connection, workspace_id)
    }

    pub fn get_workspace(&self, workspace_id: Uuid) -> Result<WorkspaceRecord, StorageError> {
        let _activity = self.read_activity()?;
        let connection = open_connection(&self.inner)?;
        self.workspace_from_connection(&connection, workspace_id)
    }

    pub fn create_session(
        &self,
        workspace_id: Uuid,
        session_id: Uuid,
        created_at: DateTime<Utc>,
    ) -> Result<SessionRecord, StorageError> {
        validate_parent_active(&self.inner, workspace_id, "workspace")?;
        validate_id(session_id, "session")?;
        let _activity = self.write_activity()?;
        let connection = open_connection(&self.inner)?;
        connection
            .execute(
                "INSERT INTO cp_sessions
                 (id, workspace_id, state, created_at_utc, updated_at_utc)
                 VALUES (?1, ?2, 'open', ?3, ?3)",
                params![
                    session_id.to_string(),
                    workspace_id.to_string(),
                    timestamp(&created_at)
                ],
            )
            .map_err(|error| map_constraint(error, session_id))?;
        self.session_from_connection(&connection, session_id)
    }

    pub fn get_session(&self, session_id: Uuid) -> Result<SessionRecord, StorageError> {
        let _activity = self.read_activity()?;
        let connection = open_connection(&self.inner)?;
        self.session_from_connection(&connection, session_id)
    }

    pub fn transition_session(
        &self,
        session_id: Uuid,
        target: SessionState,
        updated_at: DateTime<Utc>,
    ) -> Result<SessionRecord, StorageError> {
        let _activity = self.write_activity()?;
        let connection = open_connection(&self.inner)?;
        let current: Option<(String, String, String)> = connection
            .query_row(
                "SELECT state, created_at_utc, updated_at_utc
                 FROM cp_sessions WHERE id = ?1",
                params![session_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(|_| StorageError::database("read Session transition"))?;
        let Some((state, created_at, previous_update)) = current else {
            return Err(StorageError::NotFound(session_id));
        };
        let from = parse_session_state(&state)?;
        if !allowed_session_transition(from, target) {
            return Err(StorageError::InvalidDraft(
                "Session transition is not allowed",
            ));
        }
        let updated_at_text = timestamp(&updated_at);
        if updated_at_text < created_at || updated_at_text < previous_update {
            return Err(StorageError::InvalidTimestampOrder("Session update"));
        }
        if matches!(target, SessionState::Closing | SessionState::Closed) {
            let active_jobs: i64 = connection
                .query_row(
                    "SELECT COUNT(*) FROM cp_jobs
                     WHERE session_id = ?1
                       AND state NOT IN ('succeeded', 'failed', 'cancelled')",
                    params![session_id.to_string()],
                    |row| row.get(0),
                )
                .map_err(|_| StorageError::database("check Session active Jobs"))?;
            if active_jobs != 0 {
                return Err(StorageError::Busy(
                    "Session cannot close while a non-terminal Job exists",
                ));
            }
        }
        let changed = connection
            .execute(
                "UPDATE cp_sessions SET state = ?2, updated_at_utc = ?3
                 WHERE id = ?1 AND state = ?4 AND updated_at_utc = ?5",
                params![
                    session_id.to_string(),
                    session_state_text(target),
                    updated_at_text,
                    state,
                    previous_update
                ],
            )
            .map_err(|_| StorageError::database("persist Session transition"))?;
        if changed != 1 {
            return Err(StorageError::Busy("Session changed while transitioning"));
        }
        self.session_from_connection(&connection, session_id)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_source_connection(
        &self,
        workspace_id: Uuid,
        connection_id: Uuid,
        kind: ConnectorKind,
        name: impl Into<String>,
        safe_config: Value,
        credential_ref: CredentialRef,
        created_at: DateTime<Utc>,
    ) -> Result<SourceConnectionRecord, StorageError> {
        validate_parent_active(&self.inner, workspace_id, "workspace")?;
        validate_id(connection_id, "source connection")?;
        let name = name.into();
        validate_safe_text(&name, "SourceConnection name")?;
        validate_safe_json(&safe_config, false)?;
        let config_json = compact_json(&safe_config, "serialize source configuration")?;
        let credential_ref = credential_ref.as_str();
        if !credential_ref.starts_with("cred://") || contains_secret_marker(credential_ref) {
            return Err(StorageError::InvalidDraft(
                "credential reference must be an opaque cred:// reference",
            ));
        }
        let _activity = self.write_activity()?;
        let connection = open_connection(&self.inner)?;
        connection
            .execute(
                "INSERT INTO cp_connections
                 (id, workspace_id, connector_kind, name, config_json, credential_ref,
                  state, created_at_utc, updated_at_utc)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'active', ?7, ?7)",
                params![
                    connection_id.to_string(),
                    workspace_id.to_string(),
                    enum_json(kind)?,
                    name,
                    config_json,
                    credential_ref,
                    timestamp(&created_at)
                ],
            )
            .map_err(|error| map_constraint(error, connection_id))?;
        self.source_connection_from_connection(&connection, connection_id)
    }

    pub fn get_source_connection(
        &self,
        connection_id: Uuid,
    ) -> Result<SourceConnectionRecord, StorageError> {
        let _activity = self.read_activity()?;
        let connection = open_connection(&self.inner)?;
        self.source_connection_from_connection(&connection, connection_id)
    }

    pub fn transition_source_connection(
        &self,
        connection_id: Uuid,
        target: SourceConnectionState,
        updated_at: DateTime<Utc>,
    ) -> Result<SourceConnectionRecord, StorageError> {
        let _activity = self.write_activity()?;
        let connection = open_connection(&self.inner)?;
        let current: Option<(String, String)> = connection
            .query_row(
                "SELECT state, created_at_utc FROM cp_connections WHERE id = ?1",
                params![connection_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|_| StorageError::database("read SourceConnection transition"))?;
        let Some((state, created_at)) = current else {
            return Err(StorageError::NotFound(connection_id));
        };
        let from = parse_source_connection_state(&state)?;
        if !allowed_source_connection_transition(from, target) {
            return Err(StorageError::InvalidDraft(
                "SourceConnection transition is not allowed",
            ));
        }
        let updated_at_text = timestamp(&updated_at);
        if updated_at_text < created_at {
            return Err(StorageError::InvalidTimestampOrder(
                "SourceConnection update",
            ));
        }
        let changed = connection
            .execute(
                "UPDATE cp_connections SET state = ?2, updated_at_utc = ?3
                 WHERE id = ?1 AND state = ?4",
                params![
                    connection_id.to_string(),
                    source_connection_state_text(target),
                    updated_at_text,
                    state
                ],
            )
            .map_err(|_| StorageError::database("persist SourceConnection transition"))?;
        if changed != 1 {
            return Err(StorageError::Busy(
                "SourceConnection changed while transitioning",
            ));
        }
        self.source_connection_from_connection(&connection, connection_id)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_source_asset(
        &self,
        workspace_id: Uuid,
        connection_id: Uuid,
        asset_id: Uuid,
        kind: AssetKind,
        name: impl Into<String>,
        safe_locator: Value,
        discovered_at: DateTime<Utc>,
    ) -> Result<SourceAssetRecord, StorageError> {
        validate_id(asset_id, "source asset")?;
        let name = name.into();
        validate_safe_text(&name, "SourceAsset name")?;
        validate_safe_json(&safe_locator, false)?;
        let locator_json = compact_json(&safe_locator, "serialize asset locator")?;
        let _activity = self.write_activity()?;
        let connection = open_connection(&self.inner)?;
        ensure_workspace_active_connection(&connection, workspace_id)?;
        ensure_connection_workspace(&connection, workspace_id, connection_id)?;
        connection
            .execute(
                "INSERT INTO cp_assets
                 (id, workspace_id, connection_id, asset_kind, name, locator_json,
                  state, discovered_at_utc)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'active', ?7)",
                params![
                    asset_id.to_string(),
                    workspace_id.to_string(),
                    connection_id.to_string(),
                    enum_json(kind)?,
                    name,
                    locator_json,
                    timestamp(&discovered_at)
                ],
            )
            .map_err(|error| map_constraint(error, asset_id))?;
        self.source_asset_from_connection(&connection, asset_id)
    }

    pub fn get_source_asset(&self, asset_id: Uuid) -> Result<SourceAssetRecord, StorageError> {
        let _activity = self.read_activity()?;
        let connection = open_connection(&self.inner)?;
        self.source_asset_from_connection(&connection, asset_id)
    }

    pub fn retire_source_asset(&self, asset_id: Uuid) -> Result<SourceAssetRecord, StorageError> {
        let _activity = self.write_activity()?;
        let connection = open_connection(&self.inner)?;
        let changed = connection
            .execute(
                "UPDATE cp_assets SET state = 'retired'
                 WHERE id = ?1 AND state = 'active'",
                params![asset_id.to_string()],
            )
            .map_err(|_| StorageError::database("retire SourceAsset"))?;
        if changed != 1 {
            return Err(StorageError::InvalidDraft(
                "only an active SourceAsset can be retired",
            ));
        }
        self.source_asset_from_connection(&connection, asset_id)
    }

    pub fn create_dataset(
        &self,
        workspace_id: Uuid,
        session_id: Uuid,
        source_asset_id: Uuid,
        dataset_id: Uuid,
        name: impl Into<String>,
        created_at: DateTime<Utc>,
    ) -> Result<DatasetRecord, StorageError> {
        validate_id(dataset_id, "dataset")?;
        let name = name.into();
        validate_safe_text(&name, "Dataset name")?;
        let _activity = self.write_activity()?;
        let connection = open_connection(&self.inner)?;
        ensure_workspace_active_connection(&connection, workspace_id)?;
        ensure_session_workspace(&connection, workspace_id, session_id)?;
        ensure_asset_workspace(&connection, workspace_id, source_asset_id)?;
        connection
            .execute(
                "INSERT INTO cp_datasets
                 (id, workspace_id, session_id, source_asset_id, name, state, created_at_utc)
                 VALUES (?1, ?2, ?3, ?4, ?5, 'active', ?6)",
                params![
                    dataset_id.to_string(),
                    workspace_id.to_string(),
                    session_id.to_string(),
                    source_asset_id.to_string(),
                    name,
                    timestamp(&created_at)
                ],
            )
            .map_err(|error| map_constraint(error, dataset_id))?;
        self.dataset_from_connection(&connection, dataset_id)
    }

    pub fn get_dataset(&self, dataset_id: Uuid) -> Result<DatasetRecord, StorageError> {
        let _activity = self.read_activity()?;
        let connection = open_connection(&self.inner)?;
        self.dataset_from_connection(&connection, dataset_id)
    }

    pub fn archive_dataset(&self, dataset_id: Uuid) -> Result<DatasetRecord, StorageError> {
        let _activity = self.write_activity()?;
        let connection = open_connection(&self.inner)?;
        let changed = connection
            .execute(
                "UPDATE cp_datasets SET state = 'archived'
                 WHERE id = ?1 AND state = 'active'",
                params![dataset_id.to_string()],
            )
            .map_err(|_| StorageError::database("archive Dataset"))?;
        if changed != 1 {
            return Err(StorageError::InvalidDraft(
                "only an active Dataset can be archived",
            ));
        }
        self.dataset_from_connection(&connection, dataset_id)
    }

    pub fn create_plan(
        &self,
        workspace_id: Uuid,
        plan_id: Uuid,
        created_at: DateTime<Utc>,
    ) -> Result<PlanRecord, StorageError> {
        validate_id(plan_id, "plan")?;
        validate_parent_active(&self.inner, workspace_id, "workspace")?;
        let _activity = self.write_activity()?;
        let connection = open_connection(&self.inner)?;
        connection
            .execute(
                "INSERT INTO cp_plans
                 (id, workspace_id, state, current_version_id, created_at_utc, updated_at_utc)
                 VALUES (?1, ?2, 'active', NULL, ?3, ?3)",
                params![
                    plan_id.to_string(),
                    workspace_id.to_string(),
                    timestamp(&created_at)
                ],
            )
            .map_err(|error| map_constraint(error, plan_id))?;
        self.plan_from_connection(&connection, plan_id)
    }

    pub fn get_plan(&self, plan_id: Uuid) -> Result<PlanRecord, StorageError> {
        let _activity = self.read_activity()?;
        let connection = open_connection(&self.inner)?;
        self.plan_from_connection(&connection, plan_id)
    }

    pub fn archive_plan(&self, plan_id: Uuid) -> Result<PlanRecord, StorageError> {
        let _activity = self.write_activity()?;
        let connection = open_connection(&self.inner)?;
        let changed = connection
            .execute(
                "UPDATE cp_plans SET state = 'archived'
                 WHERE id = ?1 AND state = 'active'",
                params![plan_id.to_string()],
            )
            .map_err(|_| StorageError::database("archive Plan"))?;
        if changed != 1 {
            return Err(StorageError::InvalidDraft(
                "only an active Plan can be archived",
            ));
        }
        self.plan_from_connection(&connection, plan_id)
    }

    pub fn create_plan_version(
        &self,
        draft: PlanVersionDraft,
    ) -> Result<PlanVersionRecord, StorageError> {
        draft.validate()?;
        let _activity = self.write_activity()?;
        let connection = open_connection(&self.inner)?;
        ensure_workspace_active_connection(&connection, draft.workspace_id)?;
        ensure_plan_workspace(&connection, draft.workspace_id, draft.plan_id)?;
        if let Some(parent) = draft.parent_version_id {
            ensure_parent_plan_version(&connection, draft.workspace_id, draft.plan_id, parent)?;
        }
        let canonical_digest = sha256_hex(&draft.canonical_plan_bytes);
        if canonical_digest != digest_hex(&draft.canonical_plan_digest) {
            return Err(StorageError::Serialization(
                "canonical plan digest does not match canonical bytes",
            ));
        }
        validate_safe_json(&draft.logical_plan, false)?;
        let logical_plan_json = compact_json(&draft.logical_plan, "serialize logical plan")?;
        connection
            .execute(
                "INSERT INTO cp_plan_versions
                 (id, plan_id, workspace_id, version_number, parent_version_id,
                  logical_plan_json, canonical_plan_bytes, canonical_plan_digest,
                  plan_fingerprint, state, created_at_utc, published_at_utc, archived_at_utc)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'draft', ?10, NULL, NULL)",
                params![
                    draft.plan_version_id.to_string(),
                    draft.plan_id.to_string(),
                    draft.workspace_id.to_string(),
                    i64::from(draft.version_number),
                    draft.parent_version_id.map(|id| id.to_string()),
                    logical_plan_json,
                    draft.canonical_plan_bytes,
                    canonical_digest,
                    digest_hex(&draft.plan_fingerprint),
                    timestamp(&draft.created_at)
                ],
            )
            .map_err(|error| map_constraint(error, draft.plan_version_id))?;
        self.plan_version_from_connection(&connection, draft.plan_version_id)
    }

    pub fn publish_plan_version(
        &self,
        plan_version_id: Uuid,
        expected_current_version_id: Option<Uuid>,
        published_at: DateTime<Utc>,
    ) -> Result<PlanVersionRecord, StorageError> {
        let _activity = self.write_activity()?;
        let mut connection = open_connection(&self.inner)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StorageError::database("begin publish plan version"))?;
        let version = plan_version_raw(&transaction, plan_version_id)?;
        if version.state != "draft" {
            return Err(StorageError::InvalidDraft(
                "only a draft PlanVersion can be published",
            ));
        }
        let plan: Option<(String, Option<String>)> = transaction
            .query_row(
                "SELECT state, current_version_id FROM cp_plans WHERE id = ?1",
                params![version.plan_id.clone()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|_| StorageError::database("read current plan version"))?;
        let Some((plan_state, current)) = plan else {
            return Err(StorageError::NotFound(parse_uuid(&version.plan_id)?));
        };
        if plan_state != "active" {
            return Err(StorageError::InvalidDraft(
                "archived Plan cannot publish a PlanVersion",
            ));
        }
        let current_id = current.as_deref().map(parse_uuid).transpose()?;
        if current_id != expected_current_version_id {
            return Err(StorageError::InvalidDraft(
                "PlanVersion expected-current CAS conflict",
            ));
        }
        if sha256_hex(&version.canonical_plan_bytes) != version.canonical_plan_digest {
            return Err(StorageError::Serialization(
                "stored canonical plan digest does not match bytes",
            ));
        }
        let created_at =
            parse_timestamp(&version.created_at_utc, "PlanVersion creation timestamp")?;
        if published_at < created_at {
            return Err(StorageError::InvalidTimestampOrder(
                "PlanVersion publication",
            ));
        }
        if let Some(current_id) = current {
            transaction
                .execute(
                    "UPDATE cp_plan_versions SET state = 'superseded'
                     WHERE id = ?1 AND state = 'published'",
                    params![current_id],
                )
                .map_err(|_| StorageError::database("supersede plan version"))?;
        }
        let changed = transaction
            .execute(
                "UPDATE cp_plan_versions
                 SET state = 'published', published_at_utc = ?2
                 WHERE id = ?1 AND state = 'draft'",
                params![plan_version_id.to_string(), timestamp(&published_at)],
            )
            .map_err(|_| StorageError::database("publish plan version"))?;
        if changed != 1 {
            return Err(StorageError::InvalidDraft(
                "PlanVersion changed while publishing",
            ));
        }
        transaction
            .execute(
                "UPDATE cp_plans SET current_version_id = ?2, updated_at_utc = ?3 WHERE id = ?1",
                params![
                    version.plan_id,
                    plan_version_id.to_string(),
                    timestamp(&published_at)
                ],
            )
            .map_err(|_| StorageError::database("advance current plan version"))?;
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit publish plan version"))?;
        self.plan_version_from_connection(&connection, plan_version_id)
    }

    pub fn archive_plan_version(
        &self,
        plan_version_id: Uuid,
        archived_at: DateTime<Utc>,
    ) -> Result<PlanVersionRecord, StorageError> {
        let _activity = self.write_activity()?;
        let mut connection = open_connection(&self.inner)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StorageError::database("begin archive PlanVersion"))?;
        let version = plan_version_raw(&transaction, plan_version_id)?;
        if version.state != "superseded" {
            return Err(StorageError::InvalidDraft(
                "only a superseded PlanVersion can be archived",
            ));
        }
        let created_at =
            parse_timestamp(&version.created_at_utc, "PlanVersion creation timestamp")?;
        if archived_at < created_at {
            return Err(StorageError::InvalidTimestampOrder("PlanVersion archive"));
        }
        transaction
            .execute(
                "UPDATE cp_plan_versions SET state = 'archived', archived_at_utc = ?2
                 WHERE id = ?1 AND state = 'superseded'",
                params![plan_version_id.to_string(), timestamp(&archived_at)],
            )
            .map_err(|_| StorageError::database("persist PlanVersion archive"))?;
        let record = plan_version_from_raw(plan_version_raw(&transaction, plan_version_id)?)?;
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit PlanVersion archive"))?;
        Ok(record)
    }

    pub fn get_plan_version(
        &self,
        plan_version_id: Uuid,
    ) -> Result<PlanVersionRecord, StorageError> {
        let _activity = self.read_activity()?;
        let connection = open_connection(&self.inner)?;
        self.plan_version_from_connection(&connection, plan_version_id)
    }

    pub fn submit_job(&self, submission: JobSubmission) -> Result<SubmitOutcome, StorageError> {
        submission.validate()?;
        let _activity = self.write_activity()?;
        let mut connection = open_connection(&self.inner)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StorageError::database("begin submit job"))?;

        let existing: Option<(String, String)> = transaction
            .query_row(
                "SELECT request_digest, job_id FROM cp_idempotency_keys
                 WHERE workspace_id = ?1 AND operation = ?2 AND idempotency_key = ?3",
                params![
                    submission.workspace_id.to_string(),
                    OPERATION_JOB_SUBMIT,
                    submission.idempotency_key
                ],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|_| StorageError::database("read job idempotency key"))?;
        if let Some((stored_digest, original_job)) = existing {
            let original_job = parse_uuid(&original_job)?;
            if stored_digest != digest_hex(&submission.request_digest) {
                return Err(StorageError::AlreadyExists(original_job));
            }
            let result_json: String = transaction
                .query_row(
                    "SELECT result_json FROM cp_idempotency_keys
                     WHERE workspace_id = ?1 AND operation = ?2 AND idempotency_key = ?3",
                    params![
                        submission.workspace_id.to_string(),
                        OPERATION_JOB_SUBMIT,
                        submission.idempotency_key
                    ],
                    |row| row.get(0),
                )
                .map_err(|_| StorageError::database("read idempotency result"))?;
            let result = parse_json(&result_json, "idempotency result")?;
            validate_safe_json(&result, false)?;
            let job = job_from_transaction(&transaction, original_job)?;
            transaction
                .commit()
                .map_err(|_| StorageError::database("commit idempotent job replay"))?;
            return Ok(SubmitOutcome::Replayed(job));
        }

        ensure_workspace_active(&transaction, submission.workspace_id)?;
        ensure_session_workspace(&transaction, submission.workspace_id, submission.session_id)?;
        ensure_plan_version_for_job(
            &transaction,
            submission.workspace_id,
            submission.plan_version_id,
            &submission.canonical_plan_digest,
        )?;
        validate_submission_timestamp(
            &transaction,
            submission.session_id,
            submission.plan_version_id,
            submission.queued_at,
        )?;
        let queued: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM cp_jobs WHERE workspace_id = ?1 AND state = 'queued'",
                params![submission.workspace_id.to_string()],
                |row| row.get(0),
            )
            .map_err(|_| StorageError::database("count queued jobs"))?;
        if usize::try_from(queued).unwrap_or(usize::MAX) >= MAX_QUEUED_JOBS_PER_WORKSPACE {
            return Err(StorageError::Busy("control-plane queue is full"));
        }

        let input_json = compact_json(&submission.inputs, "serialize job inputs")?;
        let execution_policy_json =
            compact_json(&submission.execution_policy, "serialize execution policy")?;
        let output_policy_json =
            compact_json(&submission.output_policy, "serialize output policy")?;
        let result_json = serde_json::json!({
            "jobId": submission.job_id,
            "state": "queued"
        })
        .to_string();
        transaction
            .execute(
                "INSERT INTO cp_jobs
                 (id, workspace_id, session_id, plan_version_id, canonical_plan_digest,
                  input_json, execution_policy_json, output_policy_json, state,
                  queued_at_utc, started_at_utc, finished_at_utc, run_id, failure_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'queued', ?9, NULL, NULL, NULL, NULL)",
                params![
                    submission.job_id.to_string(),
                    submission.workspace_id.to_string(),
                    submission.session_id.to_string(),
                    submission.plan_version_id.to_string(),
                    digest_hex(&submission.canonical_plan_digest),
                    input_json,
                    execution_policy_json,
                    output_policy_json,
                    timestamp(&submission.queued_at)
                ],
            )
            .map_err(|error| map_constraint(error, submission.job_id))?;
        transaction
            .execute(
                "INSERT INTO cp_idempotency_keys
                 (workspace_id, operation, idempotency_key, request_digest, job_id,
                  result_json, created_at_utc)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    submission.workspace_id.to_string(),
                    OPERATION_JOB_SUBMIT,
                    submission.idempotency_key,
                    digest_hex(&submission.request_digest),
                    submission.job_id.to_string(),
                    result_json,
                    timestamp(&submission.queued_at)
                ],
            )
            .map_err(|error| map_constraint(error, submission.job_id))?;
        append_event_tx(
            &transaction,
            EventDraft {
                event_id: submission.event_id,
                stream_kind: EventStreamKind::Job,
                stream_id: submission.job_id,
                job_id: submission.job_id,
                run_id: None,
                event_type: ControlPlaneEventType::JobQueued,
                event_version: EVENT_VERSION,
                occurred_at: submission.queued_at,
                request_id: submission.request_id,
                correlation_id: submission.correlation_id,
                actor_ref: submission.actor_ref,
                payload: serde_json::json!({"state": "queued"}),
            },
        )?;
        let job = job_from_transaction(&transaction, submission.job_id)?;
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit job submission"))?;
        Ok(SubmitOutcome::Created(job))
    }

    pub fn get_job(&self, job_id: Uuid) -> Result<JobRecord, StorageError> {
        let _activity = self.read_activity()?;
        let connection = open_connection(&self.inner)?;
        job_from_connection(&connection, job_id)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn claim_job(
        &self,
        job_id: Uuid,
        run_id: Uuid,
        started_at: DateTime<Utc>,
        engine_contract_version: u16,
        engine_build: impl Into<String>,
        job_event: EventDraft,
        run_event: EventDraft,
    ) -> Result<RunRecord, StorageError> {
        validate_id(run_id, "run")?;
        if engine_contract_version == 0 {
            return Err(StorageError::InvalidDraft(
                "engine contract version must be non-zero",
            ));
        }
        let engine_build = engine_build.into();
        if engine_build.is_empty() {
            return Err(StorageError::InvalidDraft("engine build must be non-empty"));
        }
        validate_safe_text(&engine_build, "engine build")?;
        let _activity = self.write_activity()?;
        let mut connection = open_connection(&self.inner)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StorageError::database("begin claim job"))?;
        let job = job_raw(&transaction, job_id)?;
        if job.state != "queued" {
            return Err(StorageError::Busy(
                "control-plane object was already claimed",
            ));
        }
        let queued_at = parse_timestamp(&job.queued_at_utc, "job queued timestamp")?;
        if started_at < queued_at {
            return Err(StorageError::InvalidTimestampOrder(
                "job queue and run start",
            ));
        }
        let plan_version = plan_version_raw(&transaction, parse_uuid(&job.plan_version_id)?)?;
        if job_event.stream_kind != EventStreamKind::Job
            || job_event.event_type != ControlPlaneEventType::JobRunning
            || job_event.stream_id != job_id
            || job_event.job_id != job_id
            || job_event.run_id.is_some()
        {
            return Err(StorageError::InvalidDraft("invalid Job running event"));
        }
        if run_event.stream_kind != EventStreamKind::Run
            || run_event.event_type != ControlPlaneEventType::RunRunning
            || run_event.stream_id != run_id
            || run_event.job_id != job_id
            || run_event.run_id != Some(run_id)
        {
            return Err(StorageError::InvalidDraft("invalid Run running event"));
        }
        if job_event.occurred_at < queued_at || run_event.occurred_at < started_at {
            return Err(StorageError::InvalidTimestampOrder("Job/Run start event"));
        }
        transaction
            .execute(
                "INSERT INTO cp_runs
                 (id, workspace_id, session_id, job_id, plan_id, plan_version_id,
                  canonical_plan_digest, plan_fingerprint, input_json,
                  engine_contract_version, engine_build,
                  state, started_at_utc, finished_at_utc, failure_json, snapshot_ref, bundle_ref)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 'running', ?12, NULL, NULL, NULL, NULL)",
                params![
                    run_id.to_string(),
                    job.workspace_id.clone(),
                    job.session_id.clone(),
                    job_id.to_string(),
                    plan_version.plan_id,
                    job.plan_version_id.clone(),
                    job.canonical_plan_digest.clone(),
                    plan_version.plan_fingerprint,
                    job.input_json.clone(),
                    i64::from(engine_contract_version),
                    engine_build,
                    timestamp(&started_at)
                ],
            )
            .map_err(|error| map_constraint(error, run_id))?;
        let changed = transaction
            .execute(
                "UPDATE cp_jobs SET state = 'running', started_at_utc = ?2, run_id = ?3
                 WHERE id = ?1 AND state = 'queued' AND run_id IS NULL",
                params![
                    job_id.to_string(),
                    timestamp(&started_at),
                    run_id.to_string()
                ],
            )
            .map_err(|_| StorageError::database("claim queued job"))?;
        if changed != 1 {
            return Err(StorageError::Busy(
                "control-plane object was already claimed",
            ));
        }
        append_event_tx(&transaction, job_event)?;
        append_event_tx(&transaction, run_event)?;
        let run = run_from_transaction(&transaction, run_id)?;
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit job claim"))?;
        Ok(run)
    }

    /// Applies one permitted Job transition and appends its transition event
    /// in the same SQLite transaction.
    pub fn transition_job(
        &self,
        job_id: Uuid,
        target: JobState,
        event: EventDraft,
        failure: Option<FailureInfo>,
    ) -> Result<JobRecord, StorageError> {
        let _activity = self.write_activity()?;
        let mut connection = open_connection(&self.inner)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StorageError::database("begin Job transition"))?;
        let job = job_raw(&transaction, job_id)?;
        let from = parse_job_state(&job.state)?;
        if !allowed_job_transition(from, target) {
            return Err(StorageError::InvalidDraft("Job transition is not allowed"));
        }
        validate_failure_for_target(
            target.is_terminal() && target == JobState::Failed,
            failure.as_ref(),
        )?;
        validate_state_event(&event, EventStreamKind::Job, job_id, job_id, None, target)?;
        validate_job_terminal_run(&transaction, &job, from, target)?;
        let queued_at = parse_timestamp(&job.queued_at_utc, "Job queue timestamp")?;
        let minimum_event_at = job
            .started_at_utc
            .as_deref()
            .map(|value| parse_timestamp(value, "Job start timestamp"))
            .transpose()?
            .unwrap_or(queued_at);
        if event.occurred_at < minimum_event_at {
            return Err(StorageError::InvalidTimestampOrder("Job transition"));
        }
        let failure_json = failure_json(failure.as_ref())?;
        let finished_at = target.is_terminal().then(|| timestamp(&event.occurred_at));
        let changed = transaction
            .execute(
                "UPDATE cp_jobs
                 SET state = ?2, finished_at_utc = ?3, failure_json = ?4
                 WHERE id = ?1 AND state = ?5",
                params![
                    job_id.to_string(),
                    job_state_text(target),
                    finished_at,
                    failure_json,
                    job.state
                ],
            )
            .map_err(|_| StorageError::database("persist Job transition"))?;
        if changed != 1 {
            return Err(StorageError::Busy(
                "control-plane object was already claimed",
            ));
        }
        append_event_tx(&transaction, event)?;
        let record = job_from_transaction(&transaction, job_id)?;
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit Job transition"))?;
        Ok(record)
    }

    /// Cancels a queued Job as one atomic queued -> cancelling -> cancelled
    /// transition. No Run is created and both events are committed together.
    pub fn cancel_queued_job(
        &self,
        job_id: Uuid,
        cancelling_event: EventDraft,
        cancelled_event: EventDraft,
    ) -> Result<JobRecord, StorageError> {
        let _activity = self.write_activity()?;
        let mut connection = open_connection(&self.inner)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StorageError::database("begin queued Job cancellation"))?;
        let job = job_raw(&transaction, job_id)?;
        if job.state != "queued" {
            return Err(StorageError::InvalidDraft(
                "only a queued Job can use queued cancellation",
            ));
        }
        validate_state_event(
            &cancelling_event,
            EventStreamKind::Job,
            job_id,
            job_id,
            None,
            JobState::Cancelling,
        )?;
        validate_state_event(
            &cancelled_event,
            EventStreamKind::Job,
            job_id,
            job_id,
            None,
            JobState::Cancelled,
        )?;
        let queued_at = parse_timestamp(&job.queued_at_utc, "Job queue timestamp")?;
        if cancelling_event.occurred_at < queued_at
            || cancelled_event.occurred_at < cancelling_event.occurred_at
        {
            return Err(StorageError::InvalidTimestampOrder(
                "queued Job cancellation",
            ));
        }
        transition_job_state_tx(&transaction, job_id, "queued", JobState::Cancelling, None)?;
        append_event_tx(&transaction, cancelling_event)?;
        transition_job_state_tx(
            &transaction,
            job_id,
            "cancelling",
            JobState::Cancelled,
            Some(timestamp(&cancelled_event.occurred_at)),
        )?;
        append_event_tx(&transaction, cancelled_event)?;
        let record = job_from_transaction(&transaction, job_id)?;
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit queued Job cancellation"))?;
        Ok(record)
    }

    /// Moves an active Job and its Run to cancelling atomically. The worker
    /// remains responsible for observing the cancellation and confirming the
    /// terminal state.
    pub fn cancel_running_job(
        &self,
        job_id: Uuid,
        job_event: EventDraft,
        run_event: EventDraft,
    ) -> Result<(JobRecord, RunRecord), StorageError> {
        let _activity = self.write_activity()?;
        let mut connection = open_connection(&self.inner)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StorageError::database("begin running Job cancellation"))?;
        let job = job_raw(&transaction, job_id)?;
        if job.state != "running" {
            return Err(StorageError::InvalidDraft(
                "only a running Job can use running cancellation",
            ));
        }
        let run_id = parse_uuid(
            job.run_id
                .as_deref()
                .ok_or(StorageError::Serialization("running Job has no Run"))?,
        )?;
        let run = run_raw(&transaction, run_id)?;
        if run.state != "running" || run.job_id != job_id.to_string() {
            return Err(StorageError::Serialization(
                "running Job and Run relationship is inconsistent",
            ));
        }
        validate_state_event(
            &job_event,
            EventStreamKind::Job,
            job_id,
            job_id,
            None,
            JobState::Cancelling,
        )?;
        validate_state_event(
            &run_event,
            EventStreamKind::Run,
            run_id,
            job_id,
            Some(run_id),
            RunState::Cancelling,
        )?;
        let job_started_at = job
            .started_at_utc
            .as_deref()
            .map(|value| parse_timestamp(value, "Job start timestamp"))
            .transpose()?
            .ok_or(StorageError::Serialization("running Job has no start time"))?;
        if job_event.occurred_at < job_started_at
            || run_event.occurred_at < parse_timestamp(&run.started_at_utc, "Run start timestamp")?
            || run_event.occurred_at < job_event.occurred_at
        {
            return Err(StorageError::InvalidTimestampOrder(
                "running Job cancellation",
            ));
        }
        transition_job_state_tx(&transaction, job_id, "running", JobState::Cancelling, None)?;
        transition_run_state_tx(&transaction, run_id, "running", RunState::Cancelling, None)?;
        append_event_tx(&transaction, job_event)?;
        append_event_tx(&transaction, run_event)?;
        let job_record = job_from_transaction(&transaction, job_id)?;
        let run_record = run_from_transaction(&transaction, run_id)?;
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit running Job cancellation"))?;
        Ok((job_record, run_record))
    }

    /// Applies one permitted Run transition and appends its event atomically.
    /// The paired Job terminal transition is intentionally separate unless
    /// `finish_run_and_job` is used, keeping worker orchestration out of this
    /// persistence crate.
    pub fn transition_run(
        &self,
        run_id: Uuid,
        target: RunState,
        event: EventDraft,
        failure: Option<FailureInfo>,
    ) -> Result<RunRecord, StorageError> {
        let _activity = self.write_activity()?;
        let mut connection = open_connection(&self.inner)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StorageError::database("begin Run transition"))?;
        let run = run_raw(&transaction, run_id)?;
        let from = parse_run_state(&run.state)?;
        if !allowed_run_transition(from, target) {
            return Err(StorageError::InvalidDraft("Run transition is not allowed"));
        }
        validate_failure_for_target(
            target.is_terminal() && target == RunState::Failed,
            failure.as_ref(),
        )?;
        let job_id = parse_uuid(&run.job_id)?;
        validate_state_event(
            &event,
            EventStreamKind::Run,
            run_id,
            job_id,
            Some(run_id),
            target,
        )?;
        let started_at = parse_timestamp(&run.started_at_utc, "Run start timestamp")?;
        if event.occurred_at < started_at {
            return Err(StorageError::InvalidTimestampOrder("Run transition"));
        }
        let failure_json = failure_json(failure.as_ref())?;
        let finished_at = target.is_terminal().then(|| timestamp(&event.occurred_at));
        transition_run_state_tx(&transaction, run_id, &run.state, target, finished_at)?;
        transaction
            .execute(
                "UPDATE cp_runs SET failure_json = ?2 WHERE id = ?1",
                params![run_id.to_string(), failure_json],
            )
            .map_err(|_| StorageError::database("persist Run failure"))?;
        append_event_tx(&transaction, event)?;
        let record = run_from_transaction(&transaction, run_id)?;
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit Run transition"))?;
        Ok(record)
    }

    /// Commits the terminal Run and Job outcome with both terminal events in
    /// one transaction. The first terminal CAS winner is authoritative.
    pub fn finish_run_and_job(
        &self,
        run_id: Uuid,
        run_target: RunState,
        job_target: JobState,
        run_event: EventDraft,
        job_event: EventDraft,
        failure: Option<FailureInfo>,
    ) -> Result<(RunRecord, JobRecord), StorageError> {
        let _activity = self.write_activity()?;
        let mut connection = open_connection(&self.inner)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StorageError::database("begin terminal Run and Job transition"))?;
        if !matches!(
            run_target,
            RunState::Succeeded | RunState::Failed | RunState::Cancelled
        ) || !matches!(
            job_target,
            JobState::Succeeded | JobState::Failed | JobState::Cancelled
        ) {
            return Err(StorageError::InvalidDraft(
                "terminal finish requires terminal Run and Job states",
            ));
        }
        let run = run_raw(&transaction, run_id)?;
        let job_id = parse_uuid(&run.job_id)?;
        let job = job_raw(&transaction, job_id)?;
        if (run_target == RunState::Succeeded) != (job_target == JobState::Succeeded)
            || (run_target == RunState::Failed) != (job_target == JobState::Failed)
            || (run_target == RunState::Cancelled) != (job_target == JobState::Cancelled)
        {
            return Err(StorageError::InvalidDraft(
                "Run and Job terminal states must agree",
            ));
        }
        validate_failure_for_target(run_target == RunState::Failed, failure.as_ref())?;
        if !allowed_run_transition(parse_run_state(&run.state)?, run_target)
            || !allowed_job_transition(parse_job_state(&job.state)?, job_target)
        {
            return Err(StorageError::InvalidDraft(
                "terminal state was already changed or is not allowed",
            ));
        }
        validate_state_event(
            &run_event,
            EventStreamKind::Run,
            run_id,
            job_id,
            Some(run_id),
            run_target,
        )?;
        validate_state_event(
            &job_event,
            EventStreamKind::Job,
            job_id,
            job_id,
            None,
            job_target,
        )?;
        let run_started_at = parse_timestamp(&run.started_at_utc, "Run start timestamp")?;
        let job_queued_at = parse_timestamp(&job.queued_at_utc, "Job queue timestamp")?;
        if run_event.occurred_at < run_started_at || job_event.occurred_at < job_queued_at {
            return Err(StorageError::InvalidTimestampOrder(
                "terminal Run/Job transition",
            ));
        }
        let failure_json = failure_json(failure.as_ref())?;
        let run_finished_at = timestamp(&run_event.occurred_at);
        let job_finished_at = timestamp(&job_event.occurred_at);
        transition_run_state_tx(
            &transaction,
            run_id,
            &run.state,
            run_target,
            Some(run_finished_at),
        )?;
        transaction
            .execute(
                "UPDATE cp_runs SET failure_json = ?2 WHERE id = ?1",
                params![run_id.to_string(), failure_json.clone()],
            )
            .map_err(|_| StorageError::database("persist terminal Run failure"))?;
        transition_job_state_tx(
            &transaction,
            job_id,
            &job.state,
            job_target,
            Some(job_finished_at),
        )?;
        transaction
            .execute(
                "UPDATE cp_jobs SET failure_json = ?2 WHERE id = ?1",
                params![job_id.to_string(), failure_json],
            )
            .map_err(|_| StorageError::database("persist terminal Job failure"))?;
        append_event_tx(&transaction, run_event)?;
        append_event_tx(&transaction, job_event)?;
        let run_record = run_from_transaction(&transaction, run_id)?;
        let job_record = job_from_transaction(&transaction, job_id)?;
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit terminal Run and Job transition"))?;
        Ok((run_record, job_record))
    }

    pub fn get_run(&self, run_id: Uuid) -> Result<RunRecord, StorageError> {
        let _activity = self.read_activity()?;
        let connection = open_connection(&self.inner)?;
        run_from_connection(&connection, run_id)
    }

    /// Binds committed Snapshot and VerificationBundle identities to a Run.
    /// The binding is write-once while the Run is active; terminal Run output
    /// references are immutable thereafter.
    pub fn set_run_output_refs(
        &self,
        run_id: Uuid,
        snapshot_ref: Option<Uuid>,
        bundle_ref: Option<Uuid>,
    ) -> Result<RunRecord, StorageError> {
        if let Some(snapshot_id) = snapshot_ref {
            validate_id(snapshot_id, "Snapshot reference")?;
        }
        if let Some(bundle_id) = bundle_ref {
            validate_id(bundle_id, "VerificationBundle reference")?;
        }
        let _activity = self.write_activity()?;
        let mut connection = open_connection(&self.inner)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StorageError::database("begin Run output binding"))?;
        let run = run_raw(&transaction, run_id)?;
        if run.state != "running" {
            return Err(StorageError::InvalidDraft(
                "only a running Run can bind output references",
            ));
        }
        if let Some(snapshot_id) = snapshot_ref {
            let state: Option<i64> = transaction
                .query_row(
                    "SELECT state FROM snapshots WHERE id = ?1",
                    params![snapshot_id.to_string()],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|_| StorageError::database("check Snapshot output"))?;
            if state != Some(1) {
                return Err(StorageError::InvalidDraft(
                    "Snapshot output reference is not committed",
                ));
            }
        }
        if let Some(bundle_id) = bundle_ref {
            let bundle_run_id: Option<String> = transaction
                .query_row(
                    "SELECT run_id FROM verification_bundles WHERE bundle_id = ?1",
                    params![bundle_id.to_string()],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|_| StorageError::database("check VerificationBundle output"))?;
            if bundle_run_id.as_deref().map(parse_uuid).transpose()? != Some(run_id) {
                return Err(StorageError::InvalidDraft(
                    "VerificationBundle output reference is missing or owned by another Run",
                ));
            }
        }
        if run.snapshot_ref.is_some()
            && snapshot_ref != run.snapshot_ref.as_deref().map(parse_uuid).transpose()?
        {
            return Err(StorageError::InvalidDraft(
                "Snapshot output reference is write-once",
            ));
        }
        if run.bundle_ref.is_some()
            && bundle_ref != run.bundle_ref.as_deref().map(parse_uuid).transpose()?
        {
            return Err(StorageError::InvalidDraft(
                "VerificationBundle output reference is write-once",
            ));
        }
        transaction
            .execute(
                "UPDATE cp_runs SET snapshot_ref = COALESCE(?2, snapshot_ref),
                        bundle_ref = COALESCE(?3, bundle_ref)
                 WHERE id = ?1 AND state = 'running'",
                params![
                    run_id.to_string(),
                    snapshot_ref.map(|value| value.to_string()),
                    bundle_ref.map(|value| value.to_string())
                ],
            )
            .map_err(|_| StorageError::database("persist Run output references"))?;
        let record = run_from_transaction(&transaction, run_id)?;
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit Run output binding"))?;
        Ok(record)
    }

    pub fn append_event(&self, event: EventDraft) -> Result<EventRecord, StorageError> {
        if event.event_type != ControlPlaneEventType::RunReconciled {
            return Err(StorageError::InvalidDraft(
                "lifecycle events must be appended with their state transition",
            ));
        }
        let _activity = self.write_activity()?;
        let mut connection = open_connection(&self.inner)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StorageError::database("begin event append"))?;
        let event_id = event.event_id;
        append_event_tx(&transaction, event)?;
        let record = event_from_transaction(&transaction, event_id)?;
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit event append"))?;
        Ok(record)
    }

    pub fn create_artifact_ref(
        &self,
        draft: ArtifactRefDraft,
    ) -> Result<ArtifactRefRecord, StorageError> {
        draft.validate()?;
        validate_safe_json(&draft.metadata, false)?;
        let metadata_json = compact_json(&draft.metadata, "serialize artifact metadata")?;
        let _activity = self.write_activity()?;
        let mut connection = open_connection(&self.inner)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StorageError::database("begin artifact reference"))?;
        ensure_run_workspace(&transaction, draft.workspace_id, draft.run_id)?;
        let run = run_raw(&transaction, draft.run_id)?;
        if run.state != "running" {
            return Err(StorageError::InvalidDraft(
                "ArtifactRef staging requires a running Run",
            ));
        }
        let run_started_at = parse_timestamp(&run.started_at_utc, "Run start timestamp")?;
        if draft.created_at < run_started_at {
            return Err(StorageError::InvalidTimestampOrder("ArtifactRef creation"));
        }
        transaction
            .execute(
                "INSERT INTO cp_artifact_refs
                 (id, workspace_id, run_id, artifact_kind, external_ref_kind, external_ref_id,
                  content_digest, metadata_json, state, created_at_utc, committed_at_utc,
                  tombstoned_at_utc)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'staged', ?9, NULL, NULL)",
                params![
                    draft.artifact_id.to_string(),
                    draft.workspace_id.to_string(),
                    draft.run_id.to_string(),
                    enum_json(draft.artifact_kind)?,
                    draft.external_ref_kind.as_str(),
                    draft.external_ref_id.to_string(),
                    digest_hex(&draft.content_digest),
                    metadata_json,
                    timestamp(&draft.created_at)
                ],
            )
            .map_err(|error| map_constraint(error, draft.artifact_id))?;
        let record = artifact_from_transaction(&transaction, draft.artifact_id)?;
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit artifact reference"))?;
        Ok(record)
    }

    pub fn transition_artifact_ref(
        &self,
        artifact_id: Uuid,
        target: ArtifactRefState,
        event: EventDraft,
    ) -> Result<ArtifactRefRecord, StorageError> {
        let _activity = self.write_activity()?;
        let mut connection = open_connection(&self.inner)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StorageError::database("begin artifact transition"))?;
        let artifact = artifact_raw(&transaction, artifact_id)?;
        let from = parse_artifact_state(&artifact.state)?;
        if !allowed_artifact_transition(from, target) {
            return Err(StorageError::InvalidDraft(
                "ArtifactRef transition is not allowed",
            ));
        }
        let run_id = parse_uuid(&artifact.run_id)?;
        let job_id = run_job_id(&transaction, run_id)?;
        let run = run_raw(&transaction, run_id)?;
        if target == ArtifactRefState::Committed && run.state != "running" {
            return Err(StorageError::InvalidDraft(
                "ArtifactRef commit requires a running Run",
            ));
        }
        let artifact_created_at =
            parse_timestamp(&artifact.created_at_utc, "Artifact creation timestamp")?;
        if event.occurred_at < artifact_created_at {
            return Err(StorageError::InvalidTimestampOrder(
                "ArtifactRef transition",
            ));
        }
        if let Some(committed_at) = artifact.committed_at_utc.as_deref() {
            let committed_at = parse_timestamp(committed_at, "Artifact commit timestamp")?;
            if target == ArtifactRefState::Tombstoned && event.occurred_at < committed_at {
                return Err(StorageError::InvalidTimestampOrder("ArtifactRef tombstone"));
            }
        }
        let expected_event_type = match target {
            ArtifactRefState::Committed => ControlPlaneEventType::ArtifactCommitted,
            ArtifactRefState::Tombstoned => ControlPlaneEventType::ArtifactTombstoned,
            ArtifactRefState::Staged | ArtifactRefState::Failed => {
                return Err(StorageError::InvalidDraft(
                    "ArtifactRef lifecycle event requires committed or tombstoned state",
                ));
            }
        };
        if event.stream_kind != EventStreamKind::Run
            || event.stream_id != run_id
            || event.job_id != job_id
            || event.run_id != Some(run_id)
            || event.event_type != expected_event_type
        {
            return Err(StorageError::InvalidDraft(
                "invalid Artifact lifecycle event",
            ));
        }
        let (committed_at, tombstoned_at) = match target {
            ArtifactRefState::Committed => (Some(timestamp(&event.occurred_at)), None),
            ArtifactRefState::Tombstoned => (None, Some(timestamp(&event.occurred_at))),
            ArtifactRefState::Staged | ArtifactRefState::Failed => (None, None),
        };
        let changed = transaction
            .execute(
                "UPDATE cp_artifact_refs
                 SET state = ?2, committed_at_utc = COALESCE(?3, committed_at_utc),
                     tombstoned_at_utc = COALESCE(?4, tombstoned_at_utc)
                 WHERE id = ?1 AND state = ?5",
                params![
                    artifact_id.to_string(),
                    artifact_state_text(target),
                    committed_at,
                    tombstoned_at,
                    artifact.state
                ],
            )
            .map_err(|_| StorageError::database("persist artifact transition"))?;
        if changed != 1 {
            return Err(StorageError::InvalidDraft(
                "ArtifactRef changed while transitioning",
            ));
        }
        append_event_tx(&transaction, event)?;
        let record = artifact_from_transaction(&transaction, artifact_id)?;
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit artifact transition"))?;
        Ok(record)
    }

    /// Records a failed unpublished ArtifactRef. The frozen event vocabulary
    /// has no `artifact.failed` value, so failed publication is intentionally
    /// not exposed as a readable lifecycle event.
    pub fn fail_artifact_ref(&self, artifact_id: Uuid) -> Result<ArtifactRefRecord, StorageError> {
        let _activity = self.write_activity()?;
        let mut connection = open_connection(&self.inner)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StorageError::database("begin failed artifact transition"))?;
        let artifact = artifact_raw(&transaction, artifact_id)?;
        if artifact.state != "staged" {
            return Err(StorageError::InvalidDraft(
                "only a staged ArtifactRef can fail",
            ));
        }
        transaction
            .execute(
                "UPDATE cp_artifact_refs SET state = 'failed' WHERE id = ?1 AND state = 'staged'",
                params![artifact_id.to_string()],
            )
            .map_err(|_| StorageError::database("persist failed artifact transition"))?;
        let record = artifact_from_transaction(&transaction, artifact_id)?;
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit failed artifact transition"))?;
        Ok(record)
    }

    pub fn get_artifact_ref(&self, artifact_id: Uuid) -> Result<ArtifactRefRecord, StorageError> {
        let _activity = self.read_activity()?;
        let connection = open_connection(&self.inner)?;
        readable_artifact(artifact_from_connection(&connection, artifact_id)?)
    }

    pub fn list_events(
        &self,
        workspace_id: Uuid,
        stream_kind: EventStreamKind,
        stream_id: Uuid,
        cursor: Option<EventCursor>,
        limit: usize,
    ) -> Result<EventPage, StorageError> {
        validate_page_limit(limit)?;
        if let Some(cursor) = &cursor {
            if cursor.workspace_id != workspace_id
                || cursor.stream_kind != stream_kind
                || cursor.stream_id != stream_id
            {
                return Err(StorageError::InvalidDraft(
                    "cursor is bound to another workspace or stream",
                ));
            }
        }
        let _activity = self.read_activity()?;
        let connection = open_connection(&self.inner)?;
        let after = cursor.map_or(0, |value| value.sequence);
        let mut statement = connection
            .prepare(
                "SELECT event_id, workspace_id, session_id, stream_kind, stream_id,
                        sequence, event_type, event_version, occurred_at_utc, job_id, run_id,
                        request_id, correlation_id, actor_ref, payload_json
                 FROM cp_events
                 WHERE workspace_id = ?1 AND stream_kind = ?2 AND stream_id = ?3
                   AND sequence > ?4
                 ORDER BY sequence ASC LIMIT ?5",
            )
            .map_err(|_| StorageError::database("prepare event page"))?;
        let rows = statement
            .query_map(
                params![
                    workspace_id.to_string(),
                    stream_kind_text(stream_kind),
                    stream_id.to_string(),
                    i64::try_from(after)
                        .map_err(|_| StorageError::ArithmeticOverflow("event cursor"))?,
                    i64::try_from(limit + 1)
                        .map_err(|_| StorageError::ArithmeticOverflow("event page limit"))?
                ],
                raw_event_from_row,
            )
            .map_err(|_| StorageError::database("read event page"))?;
        let mut events = Vec::with_capacity(limit);
        for row in rows {
            events.push(event_from_raw(
                row.map_err(|_| StorageError::database("decode event page"))?,
            )?);
        }
        let next = if events.len() > limit {
            events.pop();
            events.last().map(|last| EventCursor {
                workspace_id,
                stream_kind,
                stream_id,
                sequence: last.sequence,
            })
        } else {
            None
        };
        Ok(EventPage { events, next })
    }

    pub fn list_jobs(
        &self,
        workspace_id: Uuid,
        cursor: Option<JobCursor>,
        limit: usize,
    ) -> Result<JobPage, StorageError> {
        validate_page_limit(limit)?;
        if let Some(cursor) = &cursor {
            if cursor.workspace_id != workspace_id {
                return Err(StorageError::InvalidDraft(
                    "job cursor is bound to another workspace",
                ));
            }
        }
        let _activity = self.read_activity()?;
        let connection = open_connection(&self.inner)?;
        let (after_time, after_id) = cursor.map_or((String::new(), String::new()), |value| {
            (timestamp(&value.queued_at_utc), value.job_id.to_string())
        });
        let mut statement = connection
            .prepare(
                "SELECT id, workspace_id, session_id, plan_version_id, canonical_plan_digest,
                        input_json, execution_policy_json, output_policy_json, state,
                        queued_at_utc, started_at_utc, finished_at_utc, run_id, failure_json
                 FROM cp_jobs
                 WHERE workspace_id = ?1
                   AND (queued_at_utc > ?2 OR (queued_at_utc = ?2 AND id > ?3))
                 ORDER BY queued_at_utc ASC, id ASC LIMIT ?4",
            )
            .map_err(|_| StorageError::database("prepare Job page"))?;
        let rows = statement
            .query_map(
                params![
                    workspace_id.to_string(),
                    after_time,
                    after_id,
                    i64::try_from(limit + 1)
                        .map_err(|_| StorageError::ArithmeticOverflow("Job page limit"))?
                ],
                raw_job_from_row,
            )
            .map_err(|_| StorageError::database("read Job page"))?;
        let mut jobs = Vec::with_capacity(limit);
        for row in rows {
            jobs.push(job_from_raw(
                row.map_err(|_| StorageError::database("decode Job page"))?,
            )?);
        }
        let next = if jobs.len() > limit {
            jobs.pop();
            jobs.last().map(|job| JobCursor {
                workspace_id,
                queued_at_utc: job.queued_at,
                job_id: job.id,
            })
        } else {
            None
        };
        Ok(JobPage { jobs, next })
    }

    pub fn list_runs(
        &self,
        workspace_id: Uuid,
        cursor: Option<RunCursor>,
        limit: usize,
    ) -> Result<RunPage, StorageError> {
        validate_page_limit(limit)?;
        if let Some(cursor) = &cursor {
            if cursor.workspace_id != workspace_id {
                return Err(StorageError::InvalidDraft(
                    "Run cursor is bound to another workspace",
                ));
            }
        }
        let _activity = self.read_activity()?;
        let connection = open_connection(&self.inner)?;
        let (after_time, after_id) = cursor.map_or((String::new(), String::new()), |value| {
            (timestamp(&value.started_at_utc), value.run_id.to_string())
        });
        let mut statement = connection
            .prepare(
                "SELECT id, workspace_id, session_id, job_id, plan_id, plan_version_id,
                        canonical_plan_digest, plan_fingerprint, input_json,
                        engine_contract_version, engine_build,
                        state, started_at_utc, finished_at_utc, failure_json, snapshot_ref, bundle_ref
                 FROM cp_runs
                 WHERE workspace_id = ?1
                   AND (started_at_utc > ?2 OR (started_at_utc = ?2 AND id > ?3))
                 ORDER BY started_at_utc ASC, id ASC LIMIT ?4",
            )
            .map_err(|_| StorageError::database("prepare Run page"))?;
        let rows = statement
            .query_map(
                params![
                    workspace_id.to_string(),
                    after_time,
                    after_id,
                    i64::try_from(limit + 1)
                        .map_err(|_| StorageError::ArithmeticOverflow("Run page limit"))?
                ],
                raw_run_from_row,
            )
            .map_err(|_| StorageError::database("read Run page"))?;
        let mut runs = Vec::with_capacity(limit);
        for row in rows {
            runs.push(run_from_raw(
                row.map_err(|_| StorageError::database("decode Run page"))?,
            )?);
        }
        let next = if runs.len() > limit {
            runs.pop();
            runs.last().map(|run| RunCursor {
                workspace_id,
                started_at_utc: run.started_at,
                run_id: run.id,
            })
        } else {
            None
        };
        Ok(RunPage { runs, next })
    }

    pub fn list_artifact_refs(
        &self,
        workspace_id: Uuid,
        run_id: Uuid,
        cursor: Option<ArtifactCursor>,
        limit: usize,
    ) -> Result<ArtifactPage, StorageError> {
        validate_page_limit(limit)?;
        if let Some(cursor) = &cursor {
            if cursor.workspace_id != workspace_id || cursor.run_id != run_id {
                return Err(StorageError::InvalidDraft(
                    "Artifact cursor is bound to another workspace or Run",
                ));
            }
        }
        let _activity = self.read_activity()?;
        let connection = open_connection(&self.inner)?;
        ensure_run_workspace(&connection, workspace_id, run_id)?;
        let after_time = cursor.map_or_else(String::new, |value| timestamp(&value.created_at_utc));
        let after_id = cursor.map_or_else(String::new, |value| value.artifact_id.to_string());
        let mut statement = connection
            .prepare(
                "SELECT id, workspace_id, run_id, artifact_kind, external_ref_kind,
                        external_ref_id, content_digest, metadata_json, state, created_at_utc,
                        committed_at_utc, tombstoned_at_utc
                 FROM cp_artifact_refs
                 WHERE workspace_id = ?1 AND run_id = ?2
                   AND state = 'committed'
                   AND (created_at_utc > ?3 OR (created_at_utc = ?3 AND id > ?4))
                 ORDER BY created_at_utc ASC, id ASC LIMIT ?5",
            )
            .map_err(|_| StorageError::database("prepare Artifact page"))?;
        let rows = statement
            .query_map(
                params![
                    workspace_id.to_string(),
                    run_id.to_string(),
                    after_time,
                    after_id,
                    i64::try_from(limit + 1)
                        .map_err(|_| StorageError::ArithmeticOverflow("Artifact page limit"))?
                ],
                raw_artifact_from_row,
            )
            .map_err(|_| StorageError::database("read Artifact page"))?;
        let mut artifacts = Vec::with_capacity(limit);
        for row in rows {
            artifacts.push(readable_artifact(artifact_from_raw(
                row.map_err(|_| StorageError::database("decode Artifact page"))?,
            )?)?);
        }
        let next = if artifacts.len() > limit {
            artifacts.pop();
            artifacts.last().map(|artifact| ArtifactCursor {
                workspace_id,
                run_id,
                created_at_utc: artifact.created_at,
                artifact_id: artifact.artifact_id,
            })
        } else {
            None
        };
        Ok(ArtifactPage { artifacts, next })
    }

    fn write_activity(&self) -> Result<crate::ActivityGuard, StorageError> {
        acquire_activity(&self.inner, ActivityKind::Publisher)
    }

    fn read_activity(&self) -> Result<crate::ActivityGuard, StorageError> {
        acquire_activity(&self.inner, ActivityKind::Reader)
    }

    fn workspace_from_connection(
        &self,
        connection: &Connection,
        workspace_id: Uuid,
    ) -> Result<WorkspaceRecord, StorageError> {
        workspace_from_connection(connection, workspace_id)
    }

    fn session_from_connection(
        &self,
        connection: &Connection,
        session_id: Uuid,
    ) -> Result<SessionRecord, StorageError> {
        session_from_connection(connection, session_id)
    }

    fn source_connection_from_connection(
        &self,
        connection: &Connection,
        connection_id: Uuid,
    ) -> Result<SourceConnectionRecord, StorageError> {
        source_connection_from_connection(connection, connection_id)
    }

    fn source_asset_from_connection(
        &self,
        connection: &Connection,
        asset_id: Uuid,
    ) -> Result<SourceAssetRecord, StorageError> {
        source_asset_from_connection(connection, asset_id)
    }

    fn dataset_from_connection(
        &self,
        connection: &Connection,
        dataset_id: Uuid,
    ) -> Result<DatasetRecord, StorageError> {
        dataset_from_connection(connection, dataset_id)
    }

    fn plan_from_connection(
        &self,
        connection: &Connection,
        plan_id: Uuid,
    ) -> Result<PlanRecord, StorageError> {
        plan_from_connection(connection, plan_id)
    }

    fn plan_version_from_connection(
        &self,
        connection: &Connection,
        plan_version_id: Uuid,
    ) -> Result<PlanVersionRecord, StorageError> {
        plan_version_from_connection(connection, plan_version_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceRecord {
    pub id: Uuid,
    pub state: WorkspaceState,
    pub created_at: DateTime<Utc>,
    pub archived_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRecord {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub state: SessionState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SourceConnectionRecord {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub kind: ConnectorKind,
    pub name: String,
    pub safe_config: Value,
    pub credential_ref: String,
    pub state: SourceConnectionState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SourceAssetRecord {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub connection_id: Uuid,
    pub kind: AssetKind,
    pub name: String,
    pub safe_locator: Value,
    pub state: SourceAssetState,
    pub discovered_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetRecord {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub session_id: Uuid,
    pub source_asset_id: Uuid,
    pub name: String,
    pub state: DatasetState,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanRecord {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub state: PlanState,
    pub current_version_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlanVersionRecord {
    pub id: Uuid,
    pub plan_id: Uuid,
    pub workspace_id: Uuid,
    pub version_number: u32,
    pub parent_version_id: Option<Uuid>,
    pub logical_plan: Value,
    pub canonical_plan_bytes: Vec<u8>,
    pub canonical_plan_digest: [u8; 32],
    pub plan_fingerprint: [u8; 32],
    pub state: PlanVersionState,
    pub created_at: DateTime<Utc>,
    pub published_at: Option<DateTime<Utc>>,
    pub archived_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JobRecord {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub session_id: Uuid,
    pub plan_version_id: Uuid,
    pub canonical_plan_digest: [u8; 32],
    pub inputs: Vec<ControlPlaneInput>,
    pub execution_policy: Value,
    pub output_policy: Value,
    pub state: JobState,
    pub queued_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub run_id: Option<Uuid>,
    pub failure: Option<FailureInfo>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RunRecord {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub session_id: Uuid,
    pub job_id: Uuid,
    pub plan_id: Uuid,
    pub plan_version_id: Uuid,
    pub canonical_plan_digest: [u8; 32],
    pub plan_fingerprint: [u8; 32],
    pub inputs: Vec<ControlPlaneInput>,
    pub engine_contract_version: u16,
    pub engine_build: String,
    pub state: RunState,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub failure: Option<FailureInfo>,
    pub snapshot_ref: Option<Uuid>,
    pub bundle_ref: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailureInfo {
    pub category: String,
    pub retryable: bool,
    pub message: String,
}

impl FailureInfo {
    pub fn try_new(
        category: impl Into<String>,
        retryable: bool,
        message: impl Into<String>,
    ) -> Result<Self, StorageError> {
        let value = Self {
            category: category.into(),
            retryable,
            message: stillflow_core::error::sanitize_message(message.into()),
        };
        let json = serde_json::to_value(&value)
            .map_err(|_| StorageError::Serialization("serialize failure information"))?;
        validate_safe_json(&json, false)?;
        Ok(value)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct EventRecord {
    pub event_id: Uuid,
    pub workspace_id: Uuid,
    pub session_id: Uuid,
    pub stream_kind: EventStreamKind,
    pub stream_id: Uuid,
    pub sequence: u64,
    pub event_type: ControlPlaneEventType,
    pub event_version: u16,
    pub occurred_at: DateTime<Utc>,
    pub job_id: Uuid,
    pub run_id: Option<Uuid>,
    pub request_id: String,
    pub correlation_id: String,
    pub actor_ref: String,
    pub payload: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EventDraft {
    pub event_id: Uuid,
    pub stream_kind: EventStreamKind,
    pub stream_id: Uuid,
    pub job_id: Uuid,
    pub run_id: Option<Uuid>,
    pub event_type: ControlPlaneEventType,
    pub event_version: u16,
    pub occurred_at: DateTime<Utc>,
    pub request_id: String,
    pub correlation_id: String,
    pub actor_ref: String,
    pub payload: Value,
}

impl EventDraft {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        event_id: Uuid,
        stream_kind: EventStreamKind,
        stream_id: Uuid,
        job_id: Uuid,
        run_id: Option<Uuid>,
        event_type: ControlPlaneEventType,
        occurred_at: DateTime<Utc>,
        request_id: impl Into<String>,
        correlation_id: impl Into<String>,
        actor_ref: impl Into<String>,
        payload: Value,
    ) -> Self {
        Self {
            event_id,
            stream_kind,
            stream_id,
            job_id,
            run_id,
            event_type,
            event_version: EVENT_VERSION,
            occurred_at,
            request_id: request_id.into(),
            correlation_id: correlation_id.into(),
            actor_ref: actor_ref.into(),
            payload,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactRefDraft {
    pub workspace_id: Uuid,
    pub run_id: Uuid,
    pub artifact_id: Uuid,
    pub artifact_kind: ArtifactKind,
    pub external_ref_kind: ExternalRefKind,
    pub external_ref_id: Uuid,
    pub content_digest: [u8; 32],
    pub metadata: Value,
    pub created_at: DateTime<Utc>,
}

impl ArtifactRefDraft {
    fn validate(&self) -> Result<(), StorageError> {
        validate_id(self.workspace_id, "workspace")?;
        validate_id(self.run_id, "run")?;
        validate_id(self.artifact_id, "artifact")?;
        validate_id(self.external_ref_id, "artifact external reference")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExternalRefKind {
    Snapshot,
    VerificationBundle,
    Artifact,
}

impl ExternalRefKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Snapshot => "snapshot",
            Self::VerificationBundle => "verificationBundle",
            Self::Artifact => "artifact",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlanVersionDraft {
    pub workspace_id: Uuid,
    pub plan_id: Uuid,
    pub plan_version_id: Uuid,
    pub version_number: u32,
    pub parent_version_id: Option<Uuid>,
    pub logical_plan: Value,
    pub canonical_plan_bytes: Vec<u8>,
    pub canonical_plan_digest: [u8; 32],
    pub plan_fingerprint: [u8; 32],
    pub created_at: DateTime<Utc>,
}

impl PlanVersionDraft {
    fn validate(&self) -> Result<(), StorageError> {
        validate_id(self.workspace_id, "workspace")?;
        validate_id(self.plan_id, "plan")?;
        validate_id(self.plan_version_id, "PlanVersion")?;
        if self.version_number == 0 {
            return Err(StorageError::InvalidDraft(
                "PlanVersion number must be non-zero",
            ));
        }
        if self.canonical_plan_bytes.is_empty() {
            return Err(StorageError::InvalidDraft(
                "canonical plan bytes must be non-empty",
            ));
        }
        validate_secret_free_bytes(&self.canonical_plan_bytes, "canonical PlanVersion bytes")?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct JobSubmission {
    pub workspace_id: Uuid,
    pub session_id: Uuid,
    pub plan_version_id: Uuid,
    pub canonical_plan_digest: [u8; 32],
    pub job_id: Uuid,
    pub idempotency_key: String,
    pub inputs: Vec<ControlPlaneInput>,
    pub execution_policy: Value,
    pub output_policy: Value,
    pub request_digest: [u8; 32],
    pub queued_at: DateTime<Utc>,
    pub event_id: Uuid,
    pub request_id: String,
    pub correlation_id: String,
    pub actor_ref: String,
}

impl JobSubmission {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        workspace_id: Uuid,
        session_id: Uuid,
        plan_version_id: Uuid,
        canonical_plan_digest: [u8; 32],
        job_id: Uuid,
        idempotency_key: impl Into<String>,
        inputs: Vec<ControlPlaneInput>,
        execution_policy: Value,
        output_policy: Value,
        queued_at: DateTime<Utc>,
        event_id: Uuid,
        request_id: impl Into<String>,
        correlation_id: impl Into<String>,
        actor_ref: impl Into<String>,
    ) -> Result<Self, StorageError> {
        let submission = Self {
            workspace_id,
            session_id,
            plan_version_id,
            canonical_plan_digest,
            job_id,
            idempotency_key: idempotency_key.into(),
            inputs,
            execution_policy,
            output_policy,
            request_digest: [0; 32],
            queued_at,
            event_id,
            request_id: request_id.into(),
            correlation_id: correlation_id.into(),
            actor_ref: actor_ref.into(),
        };
        let request_digest = submission.compute_request_digest()?;
        Ok(Self {
            request_digest,
            ..submission
        })
    }

    fn compute_request_digest(&self) -> Result<[u8; 32], StorageError> {
        let descriptor = serde_json::json!({
            "workspaceId": self.workspace_id,
            "sessionId": self.session_id,
            "planVersionId": self.plan_version_id,
            "canonicalPlanDigest": digest_hex(&self.canonical_plan_digest),
            "inputs": self.inputs,
            "executionPolicy": self.execution_policy,
            "outputPolicy": self.output_policy,
        });
        let bytes = serde_json::to_vec(&descriptor)
            .map_err(|_| StorageError::Serialization("serialize job submission descriptor"))?;
        Ok(sha256(&bytes))
    }

    fn validate(&self) -> Result<(), StorageError> {
        validate_id(self.workspace_id, "workspace")?;
        validate_id(self.session_id, "session")?;
        validate_id(self.plan_version_id, "PlanVersion")?;
        validate_id(self.job_id, "job")?;
        validate_id(self.event_id, "event")?;
        if self.idempotency_key.is_empty() || self.idempotency_key.len() > 128 {
            return Err(StorageError::InvalidDraft(
                "idempotency key must be 1 to 128 UTF-8 bytes",
            ));
        }
        validate_safe_text(&self.idempotency_key, "idempotency key")?;
        if self.request_id.is_empty() || self.correlation_id.is_empty() || self.actor_ref.is_empty()
        {
            return Err(StorageError::InvalidDraft(
                "event identity fields must be non-empty",
            ));
        }
        validate_safe_text(&self.request_id, "request identity")?;
        validate_safe_text(&self.correlation_id, "correlation identity")?;
        validate_safe_text(&self.actor_ref, "actor identity")?;
        for input in &self.inputs {
            let id = match input.input {
                InputRef::Asset { asset_id }
                | InputRef::Snapshot {
                    snapshot_id: asset_id,
                } => asset_id,
            };
            validate_id(id, "job input")?;
        }
        validate_safe_json(&self.execution_policy, false)?;
        validate_safe_json(&self.output_policy, false)?;
        if self.compute_request_digest()? != self.request_digest {
            return Err(StorageError::InvalidDraft(
                "request digest does not match the canonical submission descriptor",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SubmitOutcome {
    Created(JobRecord),
    Replayed(JobRecord),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventCursor {
    pub workspace_id: Uuid,
    pub stream_kind: EventStreamKind,
    pub stream_id: Uuid,
    pub sequence: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EventPage {
    pub events: Vec<EventRecord>,
    pub next: Option<EventCursor>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JobCursor {
    pub workspace_id: Uuid,
    pub queued_at_utc: DateTime<Utc>,
    pub job_id: Uuid,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JobPage {
    pub jobs: Vec<JobRecord>,
    pub next: Option<JobCursor>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunCursor {
    pub workspace_id: Uuid,
    pub started_at_utc: DateTime<Utc>,
    pub run_id: Uuid,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RunPage {
    pub runs: Vec<RunRecord>,
    pub next: Option<RunCursor>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArtifactCursor {
    pub workspace_id: Uuid,
    pub run_id: Uuid,
    pub created_at_utc: DateTime<Utc>,
    pub artifact_id: Uuid,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ArtifactPage {
    pub artifacts: Vec<ArtifactRefRecord>,
    pub next: Option<ArtifactCursor>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ArtifactRefRecord {
    pub artifact_id: Uuid,
    pub workspace_id: Uuid,
    pub run_id: Uuid,
    pub artifact_kind: ArtifactKind,
    pub external_ref_kind: ExternalRefKind,
    pub external_ref_id: Uuid,
    pub content_digest: [u8; 32],
    pub metadata: Value,
    pub state: ArtifactRefState,
    pub created_at: DateTime<Utc>,
    pub committed_at: Option<DateTime<Utc>>,
    pub tombstoned_at: Option<DateTime<Utc>>,
}

#[derive(Debug)]
struct RawJob {
    id: String,
    workspace_id: String,
    session_id: String,
    plan_version_id: String,
    canonical_plan_digest: String,
    input_json: String,
    execution_policy_json: String,
    output_policy_json: String,
    state: String,
    queued_at_utc: String,
    started_at_utc: Option<String>,
    finished_at_utc: Option<String>,
    run_id: Option<String>,
    failure_json: Option<String>,
}

#[derive(Debug)]
struct RawRun {
    id: String,
    workspace_id: String,
    session_id: String,
    job_id: String,
    plan_id: String,
    plan_version_id: String,
    canonical_plan_digest: String,
    plan_fingerprint: String,
    input_json: String,
    engine_contract_version: i64,
    engine_build: String,
    state: String,
    started_at_utc: String,
    finished_at_utc: Option<String>,
    failure_json: Option<String>,
    snapshot_ref: Option<String>,
    bundle_ref: Option<String>,
}

#[derive(Debug)]
struct RawEvent {
    event_id: String,
    workspace_id: String,
    session_id: String,
    stream_kind: String,
    stream_id: String,
    sequence: i64,
    event_type: String,
    event_version: i64,
    occurred_at_utc: String,
    job_id: String,
    run_id: Option<String>,
    request_id: String,
    correlation_id: String,
    actor_ref: String,
    payload_json: String,
}

#[derive(Debug)]
struct RawArtifact {
    id: String,
    workspace_id: String,
    run_id: String,
    artifact_kind: String,
    external_ref_kind: String,
    external_ref_id: String,
    content_digest: String,
    metadata_json: String,
    state: String,
    created_at_utc: String,
    committed_at_utc: Option<String>,
    tombstoned_at_utc: Option<String>,
}

#[derive(Debug)]
struct RawPlanVersion {
    id: String,
    plan_id: String,
    workspace_id: String,
    version_number: i64,
    parent_version_id: Option<String>,
    logical_plan_json: String,
    canonical_plan_bytes: Vec<u8>,
    canonical_plan_digest: String,
    plan_fingerprint: String,
    state: String,
    created_at_utc: String,
    published_at_utc: Option<String>,
    archived_at_utc: Option<String>,
}

fn validate_id(id: Uuid, label: &'static str) -> Result<(), StorageError> {
    if id.is_nil() {
        return Err(StorageError::InvalidDraft(label));
    }
    Ok(())
}

fn timestamp(value: &DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Nanos, true)
}

fn parse_timestamp(value: &str, label: &'static str) -> Result<DateTime<Utc>, StorageError> {
    DateTime::parse_from_rfc3339(value)
        .map(|parsed| parsed.with_timezone(&Utc))
        .map_err(|_| StorageError::Serialization(label))
}

fn parse_uuid(value: &str) -> Result<Uuid, StorageError> {
    Uuid::parse_str(value).map_err(|_| StorageError::Serialization("invalid UUID reference"))
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(bytes);
    digest.finalize().into()
}

fn sha256_hex(bytes: &[u8]) -> String {
    digest_hex(&sha256(bytes))
}

fn digest_hex(value: &[u8; 32]) -> String {
    let mut result = String::with_capacity(64);
    for byte in value {
        write!(&mut result, "{byte:02x}").expect("writing to a String cannot fail");
    }
    result
}

fn parse_digest(value: &str) -> Result<[u8; 32], StorageError> {
    if value.len() != 64 {
        return Err(StorageError::Serialization(
            "digest must be 64 hex characters",
        ));
    }
    let mut digest = [0_u8; 32];
    for (index, target) in digest.iter_mut().enumerate() {
        let start = index * 2;
        *target = u8::from_str_radix(&value[start..start + 2], 16)
            .map_err(|_| StorageError::Serialization("digest contains invalid hex"))?;
    }
    Ok(digest)
}

fn compact_json<T: Serialize>(value: &T, operation: &'static str) -> Result<String, StorageError> {
    serde_json::to_string(value).map_err(|_| StorageError::Serialization(operation))
}

fn enum_json<T: Serialize>(value: T) -> Result<String, StorageError> {
    serde_json::to_string(&value).map_err(|_| StorageError::Serialization("serialize enum"))
}

fn parse_enum_json<T: for<'de> Deserialize<'de>>(
    value: &str,
    label: &'static str,
) -> Result<T, StorageError> {
    serde_json::from_str(value).map_err(|_| StorageError::Serialization(label))
}

fn parse_json(value: &str, label: &'static str) -> Result<Value, StorageError> {
    serde_json::from_str(value).map_err(|_| StorageError::Serialization(label))
}

fn validate_safe_json(value: &Value, event_payload: bool) -> Result<(), StorageError> {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                let normalized = key.to_ascii_lowercase();
                let forbidden = [
                    "password",
                    "secret",
                    "token",
                    "api_key",
                    "apikey",
                    "access_key",
                    "connection_string",
                    "raw_row",
                    "raw_data",
                    "preview_batch",
                    "backtrace",
                    "stack_trace",
                ];
                if forbidden.iter().any(|needle| normalized.contains(needle)) {
                    return Err(StorageError::InvalidDraft(
                        "secret or raw payload field is not persistable",
                    ));
                }
                if event_payload
                    && ["rows", "records", "cells", "values"]
                        .iter()
                        .any(|needle| normalized == *needle)
                {
                    return Err(StorageError::InvalidDraft(
                        "raw dataset values are not event metadata",
                    ));
                }
                validate_safe_json(child, event_payload)?;
            }
        }
        Value::Array(items) => {
            for child in items {
                validate_safe_json(child, event_payload)?;
            }
        }
        Value::String(text) if contains_secret_marker(text) => {
            return Err(StorageError::InvalidDraft(
                "secret-like string is not persistable",
            ));
        }
        _ => {}
    }
    Ok(())
}

fn contains_secret_marker(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    ["password=", "token=", "api_key=", "secret=", "bearer "]
        .iter()
        .any(|marker| lower.contains(marker))
}

fn validate_safe_text(value: &str, label: &'static str) -> Result<(), StorageError> {
    if contains_secret_marker(value) {
        return Err(StorageError::InvalidDraft(label));
    }
    Ok(())
}

fn validate_secret_free_bytes(value: &[u8], label: &'static str) -> Result<(), StorageError> {
    if contains_secret_marker(&String::from_utf8_lossy(value)) {
        return Err(StorageError::InvalidDraft(label));
    }
    Ok(())
}

fn validate_page_limit(limit: usize) -> Result<(), StorageError> {
    if limit == 0 || limit > MAX_EVENT_PAGE_SIZE {
        return Err(StorageError::InvalidDraft(
            "page size must be between 1 and 1000",
        ));
    }
    Ok(())
}

fn map_constraint(error: rusqlite::Error, id: Uuid) -> StorageError {
    match error {
        rusqlite::Error::SqliteFailure(failure, _)
            if failure.code == rusqlite::ffi::ErrorCode::ConstraintViolation =>
        {
            StorageError::AlreadyExists(id)
        }
        _ => StorageError::database("write control-plane row"),
    }
}

fn job_state_text(state: JobState) -> &'static str {
    match state {
        JobState::Queued => "queued",
        JobState::Running => "running",
        JobState::Cancelling => "cancelling",
        JobState::Succeeded => "succeeded",
        JobState::Failed => "failed",
        JobState::Cancelled => "cancelled",
    }
}

fn run_state_text(state: RunState) -> &'static str {
    match state {
        RunState::Running => "running",
        RunState::Cancelling => "cancelling",
        RunState::Succeeded => "succeeded",
        RunState::Failed => "failed",
        RunState::Cancelled => "cancelled",
    }
}

fn artifact_state_text(state: ArtifactRefState) -> &'static str {
    match state {
        ArtifactRefState::Staged => "staged",
        ArtifactRefState::Committed => "committed",
        ArtifactRefState::Tombstoned => "tombstoned",
        ArtifactRefState::Failed => "failed",
    }
}

fn stream_kind_text(kind: EventStreamKind) -> &'static str {
    match kind {
        EventStreamKind::Job => "job",
        EventStreamKind::Run => "run",
    }
}

fn parse_workspace_state(value: &str) -> Result<WorkspaceState, StorageError> {
    match value {
        "active" => Ok(WorkspaceState::Active),
        "archived" => Ok(WorkspaceState::Archived),
        _ => Err(StorageError::Serialization("unknown Workspace state")),
    }
}

fn parse_session_state(value: &str) -> Result<SessionState, StorageError> {
    match value {
        "open" => Ok(SessionState::Open),
        "closing" => Ok(SessionState::Closing),
        "closed" => Ok(SessionState::Closed),
        _ => Err(StorageError::Serialization("unknown Session state")),
    }
}

fn session_state_text(state: SessionState) -> &'static str {
    match state {
        SessionState::Open => "open",
        SessionState::Closing => "closing",
        SessionState::Closed => "closed",
    }
}

fn allowed_session_transition(from: SessionState, to: SessionState) -> bool {
    matches!(
        (from, to),
        (SessionState::Open, SessionState::Closing)
            | (SessionState::Open, SessionState::Closed)
            | (SessionState::Closing, SessionState::Closed)
    )
}

fn parse_source_connection_state(value: &str) -> Result<SourceConnectionState, StorageError> {
    match value {
        "active" => Ok(SourceConnectionState::Active),
        "disabled" => Ok(SourceConnectionState::Disabled),
        "retired" => Ok(SourceConnectionState::Retired),
        _ => Err(StorageError::Serialization(
            "unknown SourceConnection state",
        )),
    }
}

fn source_connection_state_text(state: SourceConnectionState) -> &'static str {
    match state {
        SourceConnectionState::Active => "active",
        SourceConnectionState::Disabled => "disabled",
        SourceConnectionState::Retired => "retired",
    }
}

fn allowed_source_connection_transition(
    from: SourceConnectionState,
    to: SourceConnectionState,
) -> bool {
    matches!(
        (from, to),
        (
            SourceConnectionState::Active,
            SourceConnectionState::Disabled
        ) | (
            SourceConnectionState::Disabled,
            SourceConnectionState::Active
        ) | (
            SourceConnectionState::Disabled,
            SourceConnectionState::Retired
        )
    )
}

fn parse_job_state(value: &str) -> Result<JobState, StorageError> {
    match value {
        "queued" => Ok(JobState::Queued),
        "running" => Ok(JobState::Running),
        "cancelling" => Ok(JobState::Cancelling),
        "succeeded" => Ok(JobState::Succeeded),
        "failed" => Ok(JobState::Failed),
        "cancelled" => Ok(JobState::Cancelled),
        _ => Err(StorageError::Serialization("unknown Job state")),
    }
}

fn parse_run_state(value: &str) -> Result<RunState, StorageError> {
    match value {
        "running" => Ok(RunState::Running),
        "cancelling" => Ok(RunState::Cancelling),
        "succeeded" => Ok(RunState::Succeeded),
        "failed" => Ok(RunState::Failed),
        "cancelled" => Ok(RunState::Cancelled),
        _ => Err(StorageError::Serialization("unknown Run state")),
    }
}

fn parse_artifact_state(value: &str) -> Result<ArtifactRefState, StorageError> {
    match value {
        "staged" => Ok(ArtifactRefState::Staged),
        "committed" => Ok(ArtifactRefState::Committed),
        "tombstoned" => Ok(ArtifactRefState::Tombstoned),
        "failed" => Ok(ArtifactRefState::Failed),
        _ => Err(StorageError::Serialization("unknown ArtifactRef state")),
    }
}

fn parse_stream_kind(value: &str) -> Result<EventStreamKind, StorageError> {
    match value {
        "job" => Ok(EventStreamKind::Job),
        "run" => Ok(EventStreamKind::Run),
        _ => Err(StorageError::Serialization("unknown event stream kind")),
    }
}

fn parse_event_type(value: &str) -> Result<ControlPlaneEventType, StorageError> {
    match value {
        "job.queued" => Ok(ControlPlaneEventType::JobQueued),
        "job.running" => Ok(ControlPlaneEventType::JobRunning),
        "job.cancelling" => Ok(ControlPlaneEventType::JobCancelling),
        "job.succeeded" => Ok(ControlPlaneEventType::JobSucceeded),
        "job.failed" => Ok(ControlPlaneEventType::JobFailed),
        "job.cancelled" => Ok(ControlPlaneEventType::JobCancelled),
        "run.running" => Ok(ControlPlaneEventType::RunRunning),
        "run.cancelling" => Ok(ControlPlaneEventType::RunCancelling),
        "run.succeeded" => Ok(ControlPlaneEventType::RunSucceeded),
        "run.failed" => Ok(ControlPlaneEventType::RunFailed),
        "run.cancelled" => Ok(ControlPlaneEventType::RunCancelled),
        "run.reconciled" => Ok(ControlPlaneEventType::RunReconciled),
        "artifact.committed" => Ok(ControlPlaneEventType::ArtifactCommitted),
        "artifact.tombstoned" => Ok(ControlPlaneEventType::ArtifactTombstoned),
        _ => Err(StorageError::Serialization(
            "unknown control-plane event type",
        )),
    }
}

fn allowed_job_transition(from: JobState, to: JobState) -> bool {
    matches!(
        (from, to),
        (JobState::Queued, JobState::Running)
            | (JobState::Queued, JobState::Cancelling)
            | (JobState::Queued, JobState::Failed)
            | (JobState::Running, JobState::Cancelling)
            | (JobState::Running, JobState::Succeeded)
            | (JobState::Running, JobState::Failed)
            | (JobState::Cancelling, JobState::Cancelled)
            | (JobState::Cancelling, JobState::Failed)
    )
}

fn allowed_run_transition(from: RunState, to: RunState) -> bool {
    matches!(
        (from, to),
        (RunState::Running, RunState::Succeeded)
            | (RunState::Running, RunState::Failed)
            | (RunState::Running, RunState::Cancelling)
            | (RunState::Cancelling, RunState::Cancelled)
            | (RunState::Cancelling, RunState::Failed)
    )
}

fn allowed_artifact_transition(from: ArtifactRefState, to: ArtifactRefState) -> bool {
    matches!(
        (from, to),
        (ArtifactRefState::Staged, ArtifactRefState::Committed)
            | (ArtifactRefState::Staged, ArtifactRefState::Failed)
            | (ArtifactRefState::Committed, ArtifactRefState::Tombstoned)
    )
}

fn failure_json(failure: Option<&FailureInfo>) -> Result<Option<String>, StorageError> {
    failure
        .map(|value| {
            let json = serde_json::to_value(value)
                .map_err(|_| StorageError::Serialization("serialize failure information"))?;
            validate_safe_json(&json, false)?;
            compact_json(value, "serialize failure information")
        })
        .transpose()
}

fn validate_failure_for_target(
    is_failed: bool,
    failure: Option<&FailureInfo>,
) -> Result<(), StorageError> {
    if is_failed != failure.is_some() {
        return Err(StorageError::InvalidDraft(
            "failed terminal state requires exactly one sanitized failure record",
        ));
    }
    Ok(())
}

fn workspace_from_connection(
    connection: &Connection,
    workspace_id: Uuid,
) -> Result<WorkspaceRecord, StorageError> {
    let row = connection
        .query_row(
            "SELECT id, state, created_at_utc, archived_at_utc
             FROM cp_workspaces WHERE id = ?1",
            params![workspace_id.to_string()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            },
        )
        .optional()
        .map_err(|_| StorageError::database("read Workspace"))?
        .ok_or(StorageError::NotFound(workspace_id))?;
    if parse_uuid(&row.0)? != workspace_id {
        return Err(StorageError::Serialization("Workspace identity mismatch"));
    }
    Ok(WorkspaceRecord {
        id: workspace_id,
        state: parse_workspace_state(&row.1)?,
        created_at: parse_timestamp(&row.2, "Workspace creation timestamp")?,
        archived_at: row
            .3
            .as_deref()
            .map(|value| parse_timestamp(value, "Workspace archive timestamp"))
            .transpose()?,
    })
}

fn session_from_connection(
    connection: &Connection,
    session_id: Uuid,
) -> Result<SessionRecord, StorageError> {
    let row = connection
        .query_row(
            "SELECT id, workspace_id, state, created_at_utc, updated_at_utc
             FROM cp_sessions WHERE id = ?1",
            params![session_id.to_string()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                ))
            },
        )
        .optional()
        .map_err(|_| StorageError::database("read Session"))?
        .ok_or(StorageError::NotFound(session_id))?;
    Ok(SessionRecord {
        id: parse_uuid(&row.0)?,
        workspace_id: parse_uuid(&row.1)?,
        state: parse_session_state(&row.2)?,
        created_at: parse_timestamp(&row.3, "Session creation timestamp")?,
        updated_at: parse_timestamp(&row.4, "Session update timestamp")?,
    })
}

fn source_connection_from_connection(
    connection: &Connection,
    connection_id: Uuid,
) -> Result<SourceConnectionRecord, StorageError> {
    let row = connection
        .query_row(
            "SELECT id, workspace_id, connector_kind, name, config_json, credential_ref,
                    state, created_at_utc, updated_at_utc
             FROM cp_connections WHERE id = ?1",
            params![connection_id.to_string()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, String>(8)?,
                ))
            },
        )
        .optional()
        .map_err(|_| StorageError::database("read SourceConnection"))?
        .ok_or(StorageError::NotFound(connection_id))?;
    if !row.5.starts_with("cred://") || contains_secret_marker(&row.5) {
        return Err(StorageError::Serialization(
            "SourceConnection contains an invalid credential reference",
        ));
    }
    let safe_config = parse_json(&row.4, "SourceConnection configuration")?;
    validate_safe_json(&safe_config, false)?;
    Ok(SourceConnectionRecord {
        id: parse_uuid(&row.0)?,
        workspace_id: parse_uuid(&row.1)?,
        kind: parse_enum_json(&row.2, "SourceConnection connector kind")?,
        name: row.3,
        safe_config,
        credential_ref: row.5,
        state: match row.6.as_str() {
            "active" => SourceConnectionState::Active,
            "disabled" => SourceConnectionState::Disabled,
            "retired" => SourceConnectionState::Retired,
            _ => {
                return Err(StorageError::Serialization(
                    "unknown SourceConnection state",
                ))
            }
        },
        created_at: parse_timestamp(&row.7, "SourceConnection creation timestamp")?,
        updated_at: parse_timestamp(&row.8, "SourceConnection update timestamp")?,
    })
}

fn source_asset_from_connection(
    connection: &Connection,
    asset_id: Uuid,
) -> Result<SourceAssetRecord, StorageError> {
    let row = connection
        .query_row(
            "SELECT id, workspace_id, connection_id, asset_kind, name, locator_json,
                    state, discovered_at_utc
             FROM cp_assets WHERE id = ?1",
            params![asset_id.to_string()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                ))
            },
        )
        .optional()
        .map_err(|_| StorageError::database("read SourceAsset"))?
        .ok_or(StorageError::NotFound(asset_id))?;
    let safe_locator = parse_json(&row.5, "SourceAsset locator")?;
    validate_safe_json(&safe_locator, false)?;
    Ok(SourceAssetRecord {
        id: parse_uuid(&row.0)?,
        workspace_id: parse_uuid(&row.1)?,
        connection_id: parse_uuid(&row.2)?,
        kind: parse_enum_json(&row.3, "SourceAsset kind")?,
        name: row.4,
        safe_locator,
        state: match row.6.as_str() {
            "active" => SourceAssetState::Active,
            "retired" => SourceAssetState::Retired,
            _ => return Err(StorageError::Serialization("unknown SourceAsset state")),
        },
        discovered_at: parse_timestamp(&row.7, "SourceAsset discovery timestamp")?,
    })
}

fn dataset_from_connection(
    connection: &Connection,
    dataset_id: Uuid,
) -> Result<DatasetRecord, StorageError> {
    let row = connection
        .query_row(
            "SELECT id, workspace_id, session_id, source_asset_id, name, state, created_at_utc
             FROM cp_datasets WHERE id = ?1",
            params![dataset_id.to_string()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                ))
            },
        )
        .optional()
        .map_err(|_| StorageError::database("read Dataset"))?
        .ok_or(StorageError::NotFound(dataset_id))?;
    Ok(DatasetRecord {
        id: parse_uuid(&row.0)?,
        workspace_id: parse_uuid(&row.1)?,
        session_id: parse_uuid(&row.2)?,
        source_asset_id: parse_uuid(&row.3)?,
        name: row.4,
        state: match row.5.as_str() {
            "active" => DatasetState::Active,
            "archived" => DatasetState::Archived,
            _ => return Err(StorageError::Serialization("unknown Dataset state")),
        },
        created_at: parse_timestamp(&row.6, "Dataset creation timestamp")?,
    })
}

fn plan_from_connection(
    connection: &Connection,
    plan_id: Uuid,
) -> Result<PlanRecord, StorageError> {
    let row = connection
        .query_row(
            "SELECT id, workspace_id, state, current_version_id, created_at_utc, updated_at_utc
             FROM cp_plans WHERE id = ?1",
            params![plan_id.to_string()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                ))
            },
        )
        .optional()
        .map_err(|_| StorageError::database("read Plan"))?
        .ok_or(StorageError::NotFound(plan_id))?;
    Ok(PlanRecord {
        id: parse_uuid(&row.0)?,
        workspace_id: parse_uuid(&row.1)?,
        state: match row.2.as_str() {
            "active" => PlanState::Active,
            "archived" => PlanState::Archived,
            _ => return Err(StorageError::Serialization("unknown Plan state")),
        },
        current_version_id: row.3.as_deref().map(parse_uuid).transpose()?,
        created_at: parse_timestamp(&row.4, "Plan creation timestamp")?,
        updated_at: parse_timestamp(&row.5, "Plan update timestamp")?,
    })
}

fn plan_version_raw(
    transaction: &Transaction<'_>,
    plan_version_id: Uuid,
) -> Result<RawPlanVersion, StorageError> {
    transaction
        .query_row(
            "SELECT id, plan_id, workspace_id, version_number, parent_version_id,
                    logical_plan_json, canonical_plan_bytes, canonical_plan_digest,
                    plan_fingerprint, state, created_at_utc, published_at_utc, archived_at_utc
             FROM cp_plan_versions WHERE id = ?1",
            params![plan_version_id.to_string()],
            |row| {
                Ok(RawPlanVersion {
                    id: row.get(0)?,
                    plan_id: row.get(1)?,
                    workspace_id: row.get(2)?,
                    version_number: row.get(3)?,
                    parent_version_id: row.get(4)?,
                    logical_plan_json: row.get(5)?,
                    canonical_plan_bytes: row.get(6)?,
                    canonical_plan_digest: row.get(7)?,
                    plan_fingerprint: row.get(8)?,
                    state: row.get(9)?,
                    created_at_utc: row.get(10)?,
                    published_at_utc: row.get(11)?,
                    archived_at_utc: row.get(12)?,
                })
            },
        )
        .optional()
        .map_err(|_| StorageError::database("read PlanVersion"))?
        .ok_or(StorageError::NotFound(plan_version_id))
}

fn plan_version_from_connection(
    connection: &Connection,
    plan_version_id: Uuid,
) -> Result<PlanVersionRecord, StorageError> {
    let row = connection
        .query_row(
            "SELECT id, plan_id, workspace_id, version_number, parent_version_id,
                    logical_plan_json, canonical_plan_bytes, canonical_plan_digest,
                    plan_fingerprint, state, created_at_utc, published_at_utc, archived_at_utc
             FROM cp_plan_versions WHERE id = ?1",
            params![plan_version_id.to_string()],
            |row| {
                Ok(RawPlanVersion {
                    id: row.get(0)?,
                    plan_id: row.get(1)?,
                    workspace_id: row.get(2)?,
                    version_number: row.get(3)?,
                    parent_version_id: row.get(4)?,
                    logical_plan_json: row.get(5)?,
                    canonical_plan_bytes: row.get(6)?,
                    canonical_plan_digest: row.get(7)?,
                    plan_fingerprint: row.get(8)?,
                    state: row.get(9)?,
                    created_at_utc: row.get(10)?,
                    published_at_utc: row.get(11)?,
                    archived_at_utc: row.get(12)?,
                })
            },
        )
        .optional()
        .map_err(|_| StorageError::database("read PlanVersion"))?
        .ok_or(StorageError::NotFound(plan_version_id))?;
    plan_version_from_raw(row)
}

fn plan_version_from_raw(row: RawPlanVersion) -> Result<PlanVersionRecord, StorageError> {
    let version_number = u32::try_from(row.version_number)
        .map_err(|_| StorageError::Serialization("invalid PlanVersion number"))?;
    let state = match row.state.as_str() {
        "draft" => PlanVersionState::Draft,
        "published" => PlanVersionState::Published,
        "superseded" => PlanVersionState::Superseded,
        "archived" => PlanVersionState::Archived,
        _ => return Err(StorageError::Serialization("unknown PlanVersion state")),
    };
    let logical_plan = parse_json(&row.logical_plan_json, "PlanVersion logical plan")?;
    let digest = parse_digest(&row.canonical_plan_digest)?;
    if sha256(&row.canonical_plan_bytes) != digest {
        return Err(StorageError::Serialization("PlanVersion digest mismatch"));
    }
    Ok(PlanVersionRecord {
        id: parse_uuid(&row.id)?,
        plan_id: parse_uuid(&row.plan_id)?,
        workspace_id: parse_uuid(&row.workspace_id)?,
        version_number,
        parent_version_id: row
            .parent_version_id
            .as_deref()
            .map(parse_uuid)
            .transpose()?,
        logical_plan,
        canonical_plan_bytes: row.canonical_plan_bytes,
        canonical_plan_digest: digest,
        plan_fingerprint: parse_digest(&row.plan_fingerprint)?,
        state,
        created_at: parse_timestamp(&row.created_at_utc, "PlanVersion creation timestamp")?,
        published_at: row
            .published_at_utc
            .as_deref()
            .map(|value| parse_timestamp(value, "PlanVersion publication timestamp"))
            .transpose()?,
        archived_at: row
            .archived_at_utc
            .as_deref()
            .map(|value| parse_timestamp(value, "PlanVersion archive timestamp"))
            .transpose()?,
    })
}

fn job_raw(transaction: &Transaction<'_>, job_id: Uuid) -> Result<RawJob, StorageError> {
    transaction
        .query_row(
            "SELECT id, workspace_id, session_id, plan_version_id, canonical_plan_digest,
                    input_json, execution_policy_json, output_policy_json, state,
                    queued_at_utc, started_at_utc, finished_at_utc, run_id, failure_json
             FROM cp_jobs WHERE id = ?1",
            params![job_id.to_string()],
            raw_job_from_row,
        )
        .optional()
        .map_err(|_| StorageError::database("read Job"))?
        .ok_or(StorageError::NotFound(job_id))
}

fn job_from_connection(connection: &Connection, job_id: Uuid) -> Result<JobRecord, StorageError> {
    let row = connection
        .query_row(
            "SELECT id, workspace_id, session_id, plan_version_id, canonical_plan_digest,
                    input_json, execution_policy_json, output_policy_json, state,
                    queued_at_utc, started_at_utc, finished_at_utc, run_id, failure_json
             FROM cp_jobs WHERE id = ?1",
            params![job_id.to_string()],
            raw_job_from_row,
        )
        .optional()
        .map_err(|_| StorageError::database("read Job"))?
        .ok_or(StorageError::NotFound(job_id))?;
    job_from_raw(row)
}

fn job_from_transaction(
    transaction: &Transaction<'_>,
    job_id: Uuid,
) -> Result<JobRecord, StorageError> {
    job_from_raw(job_raw(transaction, job_id)?)
}

fn job_from_raw(row: RawJob) -> Result<JobRecord, StorageError> {
    let inputs: Vec<ControlPlaneInput> = serde_json::from_str(&row.input_json)
        .map_err(|_| StorageError::Serialization("Job input references"))?;
    validate_inputs(&inputs)?;
    let execution_policy = parse_json(&row.execution_policy_json, "Job execution policy")?;
    validate_safe_json(&execution_policy, false)?;
    let output_policy = parse_json(&row.output_policy_json, "Job output policy")?;
    validate_safe_json(&output_policy, false)?;
    Ok(JobRecord {
        id: parse_uuid(&row.id)?,
        workspace_id: parse_uuid(&row.workspace_id)?,
        session_id: parse_uuid(&row.session_id)?,
        plan_version_id: parse_uuid(&row.plan_version_id)?,
        canonical_plan_digest: parse_digest(&row.canonical_plan_digest)?,
        inputs,
        execution_policy,
        output_policy,
        state: parse_job_state(&row.state)?,
        queued_at: parse_timestamp(&row.queued_at_utc, "Job queue timestamp")?,
        started_at: row
            .started_at_utc
            .as_deref()
            .map(|value| parse_timestamp(value, "Job start timestamp"))
            .transpose()?,
        finished_at: row
            .finished_at_utc
            .as_deref()
            .map(|value| parse_timestamp(value, "Job finish timestamp"))
            .transpose()?,
        run_id: row.run_id.as_deref().map(parse_uuid).transpose()?,
        failure: row.failure_json.as_deref().map(parse_failure).transpose()?,
    })
}

fn run_raw(transaction: &Transaction<'_>, run_id: Uuid) -> Result<RawRun, StorageError> {
    transaction
        .query_row(
            "SELECT id, workspace_id, session_id, job_id, plan_id, plan_version_id,
                    canonical_plan_digest, plan_fingerprint, input_json,
                    engine_contract_version, engine_build,
                    state, started_at_utc, finished_at_utc, failure_json, snapshot_ref, bundle_ref
             FROM cp_runs WHERE id = ?1",
            params![run_id.to_string()],
            raw_run_from_row,
        )
        .optional()
        .map_err(|_| StorageError::database("read Run"))?
        .ok_or(StorageError::NotFound(run_id))
}

fn run_from_connection(connection: &Connection, run_id: Uuid) -> Result<RunRecord, StorageError> {
    let row = connection
        .query_row(
            "SELECT id, workspace_id, session_id, job_id, plan_id, plan_version_id,
                    canonical_plan_digest, plan_fingerprint, input_json,
                    engine_contract_version, engine_build,
                    state, started_at_utc, finished_at_utc, failure_json, snapshot_ref, bundle_ref
             FROM cp_runs WHERE id = ?1",
            params![run_id.to_string()],
            raw_run_from_row,
        )
        .optional()
        .map_err(|_| StorageError::database("read Run"))?
        .ok_or(StorageError::NotFound(run_id))?;
    run_from_raw(row)
}

fn run_from_transaction(
    transaction: &Transaction<'_>,
    run_id: Uuid,
) -> Result<RunRecord, StorageError> {
    run_from_raw(run_raw(transaction, run_id)?)
}

fn run_from_raw(row: RawRun) -> Result<RunRecord, StorageError> {
    let inputs: Vec<ControlPlaneInput> = serde_json::from_str(&row.input_json)
        .map_err(|_| StorageError::Serialization("Run input references"))?;
    validate_inputs(&inputs)?;
    let engine_contract_version = u16::try_from(row.engine_contract_version)
        .map_err(|_| StorageError::Serialization("Run engine contract version"))?;
    if engine_contract_version == 0 || row.engine_build.is_empty() {
        return Err(StorageError::Serialization("Run execution identity"));
    }
    Ok(RunRecord {
        id: parse_uuid(&row.id)?,
        workspace_id: parse_uuid(&row.workspace_id)?,
        session_id: parse_uuid(&row.session_id)?,
        job_id: parse_uuid(&row.job_id)?,
        plan_id: parse_uuid(&row.plan_id)?,
        plan_version_id: parse_uuid(&row.plan_version_id)?,
        canonical_plan_digest: parse_digest(&row.canonical_plan_digest)?,
        plan_fingerprint: parse_digest(&row.plan_fingerprint)?,
        inputs,
        engine_contract_version,
        engine_build: row.engine_build,
        state: parse_run_state(&row.state)?,
        started_at: parse_timestamp(&row.started_at_utc, "Run start timestamp")?,
        finished_at: row
            .finished_at_utc
            .as_deref()
            .map(|value| parse_timestamp(value, "Run finish timestamp"))
            .transpose()?,
        failure: row.failure_json.as_deref().map(parse_failure).transpose()?,
        snapshot_ref: row.snapshot_ref.as_deref().map(parse_uuid).transpose()?,
        bundle_ref: row.bundle_ref.as_deref().map(parse_uuid).transpose()?,
    })
}

fn parse_failure(value: &str) -> Result<FailureInfo, StorageError> {
    let failure: FailureInfo = serde_json::from_str(value)
        .map_err(|_| StorageError::Serialization("failure information"))?;
    let json = serde_json::to_value(&failure)
        .map_err(|_| StorageError::Serialization("failure information"))?;
    validate_safe_json(&json, false)?;
    Ok(failure)
}

fn raw_job_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawJob> {
    Ok(RawJob {
        id: row.get(0)?,
        workspace_id: row.get(1)?,
        session_id: row.get(2)?,
        plan_version_id: row.get(3)?,
        canonical_plan_digest: row.get(4)?,
        input_json: row.get(5)?,
        execution_policy_json: row.get(6)?,
        output_policy_json: row.get(7)?,
        state: row.get(8)?,
        queued_at_utc: row.get(9)?,
        started_at_utc: row.get(10)?,
        finished_at_utc: row.get(11)?,
        run_id: row.get(12)?,
        failure_json: row.get(13)?,
    })
}

fn raw_run_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawRun> {
    Ok(RawRun {
        id: row.get(0)?,
        workspace_id: row.get(1)?,
        session_id: row.get(2)?,
        job_id: row.get(3)?,
        plan_id: row.get(4)?,
        plan_version_id: row.get(5)?,
        canonical_plan_digest: row.get(6)?,
        plan_fingerprint: row.get(7)?,
        input_json: row.get(8)?,
        engine_contract_version: row.get(9)?,
        engine_build: row.get(10)?,
        state: row.get(11)?,
        started_at_utc: row.get(12)?,
        finished_at_utc: row.get(13)?,
        failure_json: row.get(14)?,
        snapshot_ref: row.get(15)?,
        bundle_ref: row.get(16)?,
    })
}

fn raw_event_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawEvent> {
    Ok(RawEvent {
        event_id: row.get(0)?,
        workspace_id: row.get(1)?,
        session_id: row.get(2)?,
        stream_kind: row.get(3)?,
        stream_id: row.get(4)?,
        sequence: row.get(5)?,
        event_type: row.get(6)?,
        event_version: row.get(7)?,
        occurred_at_utc: row.get(8)?,
        job_id: row.get(9)?,
        run_id: row.get(10)?,
        request_id: row.get(11)?,
        correlation_id: row.get(12)?,
        actor_ref: row.get(13)?,
        payload_json: row.get(14)?,
    })
}

fn raw_artifact_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawArtifact> {
    Ok(RawArtifact {
        id: row.get(0)?,
        workspace_id: row.get(1)?,
        run_id: row.get(2)?,
        artifact_kind: row.get(3)?,
        external_ref_kind: row.get(4)?,
        external_ref_id: row.get(5)?,
        content_digest: row.get(6)?,
        metadata_json: row.get(7)?,
        state: row.get(8)?,
        created_at_utc: row.get(9)?,
        committed_at_utc: row.get(10)?,
        tombstoned_at_utc: row.get(11)?,
    })
}

fn artifact_raw(
    transaction: &Transaction<'_>,
    artifact_id: Uuid,
) -> Result<RawArtifact, StorageError> {
    transaction
        .query_row(
            "SELECT id, workspace_id, run_id, artifact_kind, external_ref_kind,
                    external_ref_id, content_digest, metadata_json, state, created_at_utc,
                    committed_at_utc, tombstoned_at_utc
             FROM cp_artifact_refs WHERE id = ?1",
            params![artifact_id.to_string()],
            raw_artifact_from_row,
        )
        .optional()
        .map_err(|_| StorageError::database("read ArtifactRef"))?
        .ok_or(StorageError::NotFound(artifact_id))
}

fn artifact_from_connection(
    connection: &Connection,
    artifact_id: Uuid,
) -> Result<ArtifactRefRecord, StorageError> {
    let row = connection
        .query_row(
            "SELECT id, workspace_id, run_id, artifact_kind, external_ref_kind,
                    external_ref_id, content_digest, metadata_json, state, created_at_utc,
                    committed_at_utc, tombstoned_at_utc
             FROM cp_artifact_refs WHERE id = ?1",
            params![artifact_id.to_string()],
            raw_artifact_from_row,
        )
        .optional()
        .map_err(|_| StorageError::database("read ArtifactRef"))?
        .ok_or(StorageError::NotFound(artifact_id))?;
    artifact_from_raw(row)
}

fn artifact_from_transaction(
    transaction: &Transaction<'_>,
    artifact_id: Uuid,
) -> Result<ArtifactRefRecord, StorageError> {
    artifact_from_raw(artifact_raw(transaction, artifact_id)?)
}

fn artifact_from_raw(row: RawArtifact) -> Result<ArtifactRefRecord, StorageError> {
    let metadata = parse_json(&row.metadata_json, "ArtifactRef metadata")?;
    validate_safe_json(&metadata, false)?;
    let external_ref_kind = match row.external_ref_kind.as_str() {
        "snapshot" => ExternalRefKind::Snapshot,
        "verificationBundle" => ExternalRefKind::VerificationBundle,
        "artifact" => ExternalRefKind::Artifact,
        _ => {
            return Err(StorageError::Serialization(
                "unknown ArtifactRef target kind",
            ))
        }
    };
    Ok(ArtifactRefRecord {
        artifact_id: parse_uuid(&row.id)?,
        workspace_id: parse_uuid(&row.workspace_id)?,
        run_id: parse_uuid(&row.run_id)?,
        artifact_kind: parse_enum_json(&row.artifact_kind, "Artifact kind")?,
        external_ref_kind,
        external_ref_id: parse_uuid(&row.external_ref_id)?,
        content_digest: parse_digest(&row.content_digest)?,
        metadata,
        state: parse_artifact_state(&row.state)?,
        created_at: parse_timestamp(&row.created_at_utc, "Artifact creation timestamp")?,
        committed_at: row
            .committed_at_utc
            .as_deref()
            .map(|value| parse_timestamp(value, "Artifact commit timestamp"))
            .transpose()?,
        tombstoned_at: row
            .tombstoned_at_utc
            .as_deref()
            .map(|value| parse_timestamp(value, "Artifact tombstone timestamp"))
            .transpose()?,
    })
}

fn readable_artifact(record: ArtifactRefRecord) -> Result<ArtifactRefRecord, StorageError> {
    if record.state != ArtifactRefState::Committed {
        return Err(StorageError::NotFound(record.artifact_id));
    }
    Ok(record)
}

fn event_from_transaction(
    transaction: &Transaction<'_>,
    event_id: Uuid,
) -> Result<EventRecord, StorageError> {
    let row = transaction
        .query_row(
            "SELECT event_id, workspace_id, session_id, stream_kind, stream_id,
                    sequence, event_type, event_version, occurred_at_utc, job_id, run_id,
                    request_id, correlation_id, actor_ref, payload_json
             FROM cp_events WHERE event_id = ?1",
            params![event_id.to_string()],
            raw_event_from_row,
        )
        .optional()
        .map_err(|_| StorageError::database("read Event"))?
        .ok_or(StorageError::NotFound(event_id))?;
    event_from_raw(row)
}

fn event_from_raw(row: RawEvent) -> Result<EventRecord, StorageError> {
    if row.sequence <= 0 || row.event_version <= 0 {
        return Err(StorageError::Serialization(
            "invalid Event sequence or version",
        ));
    }
    let payload = parse_json(&row.payload_json, "Event payload")?;
    validate_safe_json(&payload, true)?;
    if row.request_id.is_empty()
        || row.correlation_id.is_empty()
        || row.actor_ref.is_empty()
        || contains_secret_marker(&row.request_id)
        || contains_secret_marker(&row.correlation_id)
        || contains_secret_marker(&row.actor_ref)
        || row.payload_json.len() > MAX_EVENT_PAYLOAD_BYTES
    {
        return Err(StorageError::Serialization("unsafe Event metadata"));
    }
    let stream_kind = parse_stream_kind(&row.stream_kind)?;
    let event_type = parse_event_type(&row.event_type)?;
    if (stream_kind == EventStreamKind::Job && !event_type_is_job(event_type))
        || (stream_kind == EventStreamKind::Run && !event_type_is_run(event_type))
    {
        return Err(StorageError::Serialization(
            "Event type does not match stream kind",
        ));
    }
    Ok(EventRecord {
        event_id: parse_uuid(&row.event_id)?,
        workspace_id: parse_uuid(&row.workspace_id)?,
        session_id: parse_uuid(&row.session_id)?,
        stream_kind,
        stream_id: parse_uuid(&row.stream_id)?,
        sequence: u64::try_from(row.sequence)
            .map_err(|_| StorageError::Serialization("invalid Event sequence"))?,
        event_type,
        event_version: u16::try_from(row.event_version)
            .map_err(|_| StorageError::Serialization("invalid Event version"))?,
        occurred_at: parse_timestamp(&row.occurred_at_utc, "Event occurrence timestamp")?,
        job_id: parse_uuid(&row.job_id)?,
        run_id: row.run_id.as_deref().map(parse_uuid).transpose()?,
        request_id: row.request_id,
        correlation_id: row.correlation_id,
        actor_ref: row.actor_ref,
        payload,
    })
}

fn ensure_workspace_active(
    transaction: &Transaction<'_>,
    workspace_id: Uuid,
) -> Result<(), StorageError> {
    let state: Option<String> = transaction
        .query_row(
            "SELECT state FROM cp_workspaces WHERE id = ?1",
            params![workspace_id.to_string()],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| StorageError::database("check Workspace"))?;
    match state.as_deref() {
        Some("active") => Ok(()),
        Some("archived") => Err(StorageError::InvalidDraft(
            "archived Workspace cannot accept new control-plane objects",
        )),
        Some(_) => Err(StorageError::Serialization("unknown Workspace state")),
        None => Err(StorageError::NotFound(workspace_id)),
    }
}

fn ensure_workspace_active_connection(
    connection: &Connection,
    workspace_id: Uuid,
) -> Result<(), StorageError> {
    let state: Option<String> = connection
        .query_row(
            "SELECT state FROM cp_workspaces WHERE id = ?1",
            params![workspace_id.to_string()],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| StorageError::database("check Workspace"))?;
    match state.as_deref() {
        Some("active") => Ok(()),
        Some("archived") => Err(StorageError::InvalidDraft(
            "archived Workspace cannot accept new control-plane objects",
        )),
        Some(_) => Err(StorageError::Serialization("unknown Workspace state")),
        None => Err(StorageError::NotFound(workspace_id)),
    }
}

fn validate_parent_active(
    inner: &Arc<StoreInner>,
    parent_id: Uuid,
    label: &'static str,
) -> Result<(), StorageError> {
    let connection = open_connection(inner)?;
    let state: Option<String> = connection
        .query_row(
            "SELECT state FROM cp_workspaces WHERE id = ?1",
            params![parent_id.to_string()],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| StorageError::database("check Workspace parent"))?;
    match state.as_deref() {
        Some("active") => Ok(()),
        Some("archived") => Err(StorageError::InvalidDraft(
            "archived Workspace cannot accept new children",
        )),
        Some(_) => Err(StorageError::Serialization("unknown Workspace state")),
        None => Err(StorageError::NotFound(parent_id)),
    }
    .map_err(|error| match error {
        StorageError::NotFound(_) => StorageError::InvalidDraft(label),
        other => other,
    })
}

fn ensure_session_workspace(
    connection: &Connection,
    workspace_id: Uuid,
    session_id: Uuid,
) -> Result<(), StorageError> {
    let actual: Option<(String, String)> = connection
        .query_row(
            "SELECT workspace_id, state FROM cp_sessions WHERE id = ?1",
            params![session_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(|_| StorageError::database("check Session parent"))?;
    match actual {
        Some((value, state)) if parse_uuid(&value)? == workspace_id && state == "open" => Ok(()),
        Some((value, _)) if parse_uuid(&value)? != workspace_id => Err(StorageError::InvalidDraft(
            "Session belongs to another Workspace",
        )),
        Some(_) => Err(StorageError::InvalidDraft(
            "closing or closed Session cannot accept a new child or Job",
        )),
        None => Err(StorageError::NotFound(session_id)),
    }
}

fn ensure_connection_workspace(
    connection: &Connection,
    workspace_id: Uuid,
    connection_id: Uuid,
) -> Result<(), StorageError> {
    let actual: Option<(String, String)> = connection
        .query_row(
            "SELECT workspace_id, state FROM cp_connections WHERE id = ?1",
            params![connection_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(|_| StorageError::database("check SourceConnection parent"))?;
    match actual {
        Some((value, state)) if parse_uuid(&value)? == workspace_id && state == "active" => Ok(()),
        Some((value, _)) if parse_uuid(&value)? != workspace_id => Err(StorageError::InvalidDraft(
            "SourceConnection belongs to another Workspace",
        )),
        Some(_) => Err(StorageError::InvalidDraft(
            "retired or disabled SourceConnection cannot accept a new asset",
        )),
        None => Err(StorageError::NotFound(connection_id)),
    }
}

fn ensure_asset_workspace(
    connection: &Connection,
    workspace_id: Uuid,
    asset_id: Uuid,
) -> Result<(), StorageError> {
    let actual: Option<(String, String)> = connection
        .query_row(
            "SELECT workspace_id, state FROM cp_assets WHERE id = ?1",
            params![asset_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(|_| StorageError::database("check SourceAsset parent"))?;
    match actual {
        Some((value, state)) if parse_uuid(&value)? == workspace_id && state == "active" => Ok(()),
        Some((value, _)) if parse_uuid(&value)? != workspace_id => Err(StorageError::InvalidDraft(
            "SourceAsset belongs to another Workspace",
        )),
        Some(_) => Err(StorageError::InvalidDraft(
            "retired SourceAsset cannot accept a new Dataset",
        )),
        None => Err(StorageError::NotFound(asset_id)),
    }
}

fn ensure_plan_workspace(
    connection: &Connection,
    workspace_id: Uuid,
    plan_id: Uuid,
) -> Result<(), StorageError> {
    let actual: Option<(String, String)> = connection
        .query_row(
            "SELECT workspace_id, state FROM cp_plans WHERE id = ?1",
            params![plan_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(|_| StorageError::database("check Plan parent"))?;
    match actual {
        Some((value, state)) if parse_uuid(&value)? == workspace_id && state == "active" => Ok(()),
        Some((value, _)) if parse_uuid(&value)? != workspace_id => Err(StorageError::InvalidDraft(
            "Plan belongs to another Workspace",
        )),
        Some(_) => Err(StorageError::InvalidDraft(
            "archived Plan cannot accept a PlanVersion",
        )),
        None => Err(StorageError::NotFound(plan_id)),
    }
}

fn ensure_parent_plan_version(
    connection: &Connection,
    workspace_id: Uuid,
    plan_id: Uuid,
    parent_version_id: Uuid,
) -> Result<(), StorageError> {
    let row: Option<(String, String)> = connection
        .query_row(
            "SELECT workspace_id, plan_id FROM cp_plan_versions WHERE id = ?1",
            params![parent_version_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(|_| StorageError::database("check parent PlanVersion"))?;
    let Some((parent_workspace, parent_plan)) = row else {
        return Err(StorageError::NotFound(parent_version_id));
    };
    if parse_uuid(&parent_workspace)? != workspace_id || parse_uuid(&parent_plan)? != plan_id {
        return Err(StorageError::InvalidDraft(
            "parent PlanVersion belongs to another Plan or Workspace",
        ));
    }
    Ok(())
}

fn ensure_plan_version_for_job(
    transaction: &Transaction<'_>,
    workspace_id: Uuid,
    plan_version_id: Uuid,
    digest: &[u8; 32],
) -> Result<(), StorageError> {
    let row: Option<(String, String, String, String, Vec<u8>, String)> = transaction
        .query_row(
            "SELECT v.workspace_id, v.state, v.canonical_plan_digest,
                    v.plan_id, v.canonical_plan_bytes, p.state
             FROM cp_plan_versions v
             JOIN cp_plans p ON p.id = v.plan_id
             WHERE v.id = ?1",
            params![plan_version_id.to_string()],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .optional()
        .map_err(|_| StorageError::database("check executable PlanVersion"))?;
    let Some((stored_workspace, state, stored_digest, plan_id, bytes, plan_state)) = row else {
        return Err(StorageError::NotFound(plan_version_id));
    };
    if parse_uuid(&stored_workspace)? != workspace_id {
        return Err(StorageError::InvalidDraft(
            "PlanVersion belongs to another Workspace",
        ));
    }
    if state != "published" {
        return Err(StorageError::InvalidDraft(
            "only a published PlanVersion can bind a Job",
        ));
    }
    if plan_state != "active" {
        return Err(StorageError::InvalidDraft(
            "archived Plan cannot accept a new Job",
        ));
    }
    if parse_uuid(&plan_id)?.is_nil() {
        return Err(StorageError::Serialization("invalid Plan reference"));
    }
    if stored_digest != digest_hex(digest) || sha256_hex(&bytes) != stored_digest {
        return Err(StorageError::Serialization(
            "Job PlanVersion digest binding is invalid",
        ));
    }
    Ok(())
}

fn validate_submission_timestamp(
    transaction: &Transaction<'_>,
    session_id: Uuid,
    plan_version_id: Uuid,
    queued_at: DateTime<Utc>,
) -> Result<(), StorageError> {
    let session_created_at: String = transaction
        .query_row(
            "SELECT created_at_utc FROM cp_sessions WHERE id = ?1",
            params![session_id.to_string()],
            |row| row.get(0),
        )
        .map_err(|_| StorageError::database("read Session creation timestamp"))?;
    if queued_at < parse_timestamp(&session_created_at, "Session creation timestamp")? {
        return Err(StorageError::InvalidTimestampOrder("Job queue"));
    }
    let published_at: Option<String> = transaction
        .query_row(
            "SELECT published_at_utc FROM cp_plan_versions WHERE id = ?1",
            params![plan_version_id.to_string()],
            |row| row.get(0),
        )
        .map_err(|_| StorageError::database("read PlanVersion publication timestamp"))?;
    if let Some(published_at) = published_at {
        if queued_at < parse_timestamp(&published_at, "PlanVersion publication timestamp")? {
            return Err(StorageError::InvalidTimestampOrder("Job queue"));
        }
    }
    Ok(())
}

fn ensure_run_workspace(
    transaction_or_connection: &impl ControlPlaneQuery,
    workspace_id: Uuid,
    run_id: Uuid,
) -> Result<(), StorageError> {
    let actual = transaction_or_connection.run_workspace(run_id)?;
    match actual {
        Some(value) if parse_uuid(&value)? == workspace_id => Ok(()),
        Some(_) => Err(StorageError::InvalidDraft(
            "Run belongs to another Workspace",
        )),
        None => Err(StorageError::NotFound(run_id)),
    }
}

trait ControlPlaneQuery {
    fn run_workspace(&self, run_id: Uuid) -> Result<Option<String>, StorageError>;
}

impl ControlPlaneQuery for Connection {
    fn run_workspace(&self, run_id: Uuid) -> Result<Option<String>, StorageError> {
        self.query_row(
            "SELECT workspace_id FROM cp_runs WHERE id = ?1",
            params![run_id.to_string()],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| StorageError::database("check Run parent"))
    }
}

impl ControlPlaneQuery for Transaction<'_> {
    fn run_workspace(&self, run_id: Uuid) -> Result<Option<String>, StorageError> {
        self.query_row(
            "SELECT workspace_id FROM cp_runs WHERE id = ?1",
            params![run_id.to_string()],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| StorageError::database("check Run parent"))
    }
}

fn run_job_id(transaction: &Transaction<'_>, run_id: Uuid) -> Result<Uuid, StorageError> {
    let job_id: Option<String> = transaction
        .query_row(
            "SELECT job_id FROM cp_runs WHERE id = ?1",
            params![run_id.to_string()],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| StorageError::database("read Run Job"))?;
    job_id
        .as_deref()
        .map(parse_uuid)
        .transpose()?
        .ok_or(StorageError::NotFound(run_id))
}

fn validate_job_terminal_run(
    transaction: &Transaction<'_>,
    job: &RawJob,
    from: JobState,
    target: JobState,
) -> Result<(), StorageError> {
    if !target.is_terminal() {
        return Ok(());
    }

    let run_id = job.run_id.as_deref().map(parse_uuid).transpose()?;
    let Some(run_id) = run_id else {
        let valid_pre_run_terminal = matches!(
            (from, target),
            (JobState::Queued, JobState::Failed) | (JobState::Cancelling, JobState::Cancelled)
        );
        return if valid_pre_run_terminal {
            Ok(())
        } else {
            Err(StorageError::InvalidDraft(
                "terminal Job state requires its Run outcome",
            ))
        };
    };

    let run = run_raw(transaction, run_id)?;
    if run.job_id != job.id
        || run.workspace_id != job.workspace_id
        || run.session_id != job.session_id
    {
        return Err(StorageError::Serialization(
            "Job and Run relationship is inconsistent",
        ));
    }
    let run_state = parse_run_state(&run.state)?;
    let expected_run_state = match target {
        JobState::Succeeded => RunState::Succeeded,
        JobState::Failed => RunState::Failed,
        JobState::Cancelled => RunState::Cancelled,
        JobState::Queued | JobState::Running | JobState::Cancelling => return Ok(()),
    };
    if run_state != expected_run_state {
        return Err(StorageError::InvalidDraft(
            "terminal Job state must match its terminal Run state",
        ));
    }
    Ok(())
}

fn validate_inputs(inputs: &[ControlPlaneInput]) -> Result<(), StorageError> {
    for input in inputs {
        let id = match input.input {
            InputRef::Asset { asset_id }
            | InputRef::Snapshot {
                snapshot_id: asset_id,
            } => asset_id,
        };
        validate_id(id, "input reference")?;
    }
    Ok(())
}

fn event_type_for_job_state(state: JobState) -> Option<ControlPlaneEventType> {
    match state {
        JobState::Queued => Some(ControlPlaneEventType::JobQueued),
        JobState::Running => Some(ControlPlaneEventType::JobRunning),
        JobState::Cancelling => Some(ControlPlaneEventType::JobCancelling),
        JobState::Succeeded => Some(ControlPlaneEventType::JobSucceeded),
        JobState::Failed => Some(ControlPlaneEventType::JobFailed),
        JobState::Cancelled => Some(ControlPlaneEventType::JobCancelled),
    }
}

fn event_type_for_run_state(state: RunState) -> Option<ControlPlaneEventType> {
    match state {
        RunState::Running => Some(ControlPlaneEventType::RunRunning),
        RunState::Cancelling => Some(ControlPlaneEventType::RunCancelling),
        RunState::Succeeded => Some(ControlPlaneEventType::RunSucceeded),
        RunState::Failed => Some(ControlPlaneEventType::RunFailed),
        RunState::Cancelled => Some(ControlPlaneEventType::RunCancelled),
    }
}

fn validate_state_event<T>(
    event: &EventDraft,
    stream_kind: EventStreamKind,
    stream_id: Uuid,
    job_id: Uuid,
    run_id: Option<Uuid>,
    target: T,
) -> Result<(), StorageError>
where
    T: StateEventTarget,
{
    if event.stream_kind != stream_kind
        || event.stream_id != stream_id
        || event.job_id != job_id
        || event.run_id != run_id
        || event.event_type != target.event_type()
    {
        return Err(StorageError::InvalidDraft(
            "event does not describe the requested state transition",
        ));
    }
    validate_event_identity(event)
}

trait StateEventTarget {
    fn event_type(self) -> ControlPlaneEventType;
}

impl StateEventTarget for JobState {
    fn event_type(self) -> ControlPlaneEventType {
        event_type_for_job_state(self).expect("all Job states have event types")
    }
}

impl StateEventTarget for RunState {
    fn event_type(self) -> ControlPlaneEventType {
        event_type_for_run_state(self).expect("all Run states have event types")
    }
}

fn validate_event_identity(event: &EventDraft) -> Result<(), StorageError> {
    validate_id(event.event_id, "event")?;
    validate_id(event.stream_id, "event stream")?;
    validate_id(event.job_id, "event Job")?;
    if let Some(run_id) = event.run_id {
        validate_id(run_id, "event Run")?;
    }
    if event.event_version == 0
        || event.request_id.is_empty()
        || event.correlation_id.is_empty()
        || event.actor_ref.is_empty()
        || contains_secret_marker(&event.request_id)
        || contains_secret_marker(&event.correlation_id)
        || contains_secret_marker(&event.actor_ref)
    {
        return Err(StorageError::InvalidDraft(
            "event identity must be non-empty and secret-free",
        ));
    }
    validate_safe_json(&event.payload, true)?;
    let encoded = compact_json(&event.payload, "serialize Event payload")?;
    if encoded.len() > MAX_EVENT_PAYLOAD_BYTES {
        return Err(StorageError::InvalidDraft(
            "event payload exceeds the 64 KiB bound",
        ));
    }
    Ok(())
}

fn append_event_tx(transaction: &Transaction<'_>, event: EventDraft) -> Result<(), StorageError> {
    validate_event_identity(&event)?;
    let job = job_raw(transaction, event.job_id)?;
    let expected_workspace = job.workspace_id.clone();
    let expected_session = job.session_id.clone();
    match event.stream_kind {
        EventStreamKind::Job => {
            if event.stream_id != event.job_id
                || event.run_id.is_some()
                || !event_type_is_job(event.event_type)
            {
                return Err(StorageError::InvalidDraft(
                    "Job stream event identity is invalid",
                ));
            }
        }
        EventStreamKind::Run => {
            let run_id = event.run_id.ok_or(StorageError::InvalidDraft(
                "Run stream event must identify a Run",
            ))?;
            let run = run_raw(transaction, run_id)?;
            if event.stream_id != run_id
                || run.job_id != event.job_id.to_string()
                || run.workspace_id != expected_workspace
                || run.session_id != expected_session
                || !event_type_is_run(event.event_type)
            {
                return Err(StorageError::InvalidDraft(
                    "Run stream event belongs to another Job",
                ));
            }
        }
    }
    let payload_json = compact_json(&event.payload, "serialize Event payload")?;
    let last_sequence: i64 = transaction
        .query_row(
            "SELECT COALESCE(MAX(sequence), 0) FROM cp_events
             WHERE stream_kind = ?1 AND stream_id = ?2",
            params![
                stream_kind_text(event.stream_kind),
                event.stream_id.to_string()
            ],
            |row| row.get(0),
        )
        .map_err(|_| StorageError::database("read Event sequence"))?;
    let sequence = last_sequence
        .checked_add(1)
        .ok_or(StorageError::ArithmeticOverflow("Event sequence"))?;
    transaction
        .execute(
            "INSERT INTO cp_events
             (event_id, workspace_id, session_id, stream_kind, stream_id, sequence,
              event_type, event_version, occurred_at_utc, job_id, run_id, request_id,
              correlation_id, actor_ref, payload_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
            params![
                event.event_id.to_string(),
                expected_workspace,
                expected_session,
                stream_kind_text(event.stream_kind),
                event.stream_id.to_string(),
                sequence,
                event_type_text(event.event_type)?,
                i64::from(event.event_version),
                timestamp(&event.occurred_at),
                event.job_id.to_string(),
                event.run_id.map(|id| id.to_string()),
                event.request_id,
                event.correlation_id,
                event.actor_ref,
                payload_json
            ],
        )
        .map_err(|error| map_constraint(error, event.event_id))?;
    Ok(())
}

fn event_type_is_job(event_type: ControlPlaneEventType) -> bool {
    matches!(
        event_type,
        ControlPlaneEventType::JobQueued
            | ControlPlaneEventType::JobRunning
            | ControlPlaneEventType::JobCancelling
            | ControlPlaneEventType::JobSucceeded
            | ControlPlaneEventType::JobFailed
            | ControlPlaneEventType::JobCancelled
    )
}

fn event_type_is_run(event_type: ControlPlaneEventType) -> bool {
    matches!(
        event_type,
        ControlPlaneEventType::RunRunning
            | ControlPlaneEventType::RunCancelling
            | ControlPlaneEventType::RunSucceeded
            | ControlPlaneEventType::RunFailed
            | ControlPlaneEventType::RunCancelled
            | ControlPlaneEventType::RunReconciled
            | ControlPlaneEventType::ArtifactCommitted
            | ControlPlaneEventType::ArtifactTombstoned
    )
}

fn event_type_text(event_type: ControlPlaneEventType) -> Result<String, StorageError> {
    enum_json(event_type).map(|value| value.trim_matches('"').to_owned())
}

fn transition_job_state_tx(
    transaction: &Transaction<'_>,
    job_id: Uuid,
    expected_state: &str,
    target: JobState,
    finished_at: Option<String>,
) -> Result<(), StorageError> {
    let changed = transaction
        .execute(
            "UPDATE cp_jobs
             SET state = ?2, finished_at_utc = COALESCE(?3, finished_at_utc)
             WHERE id = ?1 AND state = ?4",
            params![
                job_id.to_string(),
                job_state_text(target),
                finished_at,
                expected_state
            ],
        )
        .map_err(|_| StorageError::database("compare-and-set Job state"))?;
    if changed != 1 {
        return Err(StorageError::Busy(
            "control-plane object was already claimed",
        ));
    }
    Ok(())
}

fn transition_run_state_tx(
    transaction: &Transaction<'_>,
    run_id: Uuid,
    expected_state: &str,
    target: RunState,
    finished_at: Option<String>,
) -> Result<(), StorageError> {
    let changed = transaction
        .execute(
            "UPDATE cp_runs
             SET state = ?2, finished_at_utc = COALESCE(?3, finished_at_utc)
             WHERE id = ?1 AND state = ?4",
            params![
                run_id.to_string(),
                run_state_text(target),
                finished_at,
                expected_state
            ],
        )
        .map_err(|_| StorageError::database("compare-and-set Run state"))?;
    if changed != 1 {
        return Err(StorageError::Busy(
            "control-plane object was already claimed",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn at(seconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(seconds, 0).expect("valid test timestamp")
    }

    struct Fixture {
        _temp: TempDir,
        store: ControlPlaneStore,
        workspace_id: Uuid,
        session_id: Uuid,
        plan_version_id: Uuid,
        plan_digest: [u8; 32],
    }

    impl Fixture {
        fn new() -> Self {
            let temp = TempDir::new().expect("temp storage");
            let store = ControlPlaneStore::open(temp.path()).expect("open control plane");
            let workspace_id = Uuid::from_u128(1);
            let session_id = Uuid::from_u128(2);
            let plan_id = Uuid::from_u128(3);
            let plan_version_id = Uuid::from_u128(4);
            store
                .create_workspace(workspace_id, at(1))
                .expect("workspace");
            store
                .create_session(workspace_id, session_id, at(2))
                .expect("session");
            store
                .create_plan(workspace_id, plan_id, at(3))
                .expect("plan");
            let canonical_plan_bytes = b"canonical-plan-v1".to_vec();
            let plan_digest = sha256(&canonical_plan_bytes);
            store
                .create_plan_version(PlanVersionDraft {
                    workspace_id,
                    plan_id,
                    plan_version_id,
                    version_number: 1,
                    parent_version_id: None,
                    logical_plan: serde_json::json!({"version": 1}),
                    canonical_plan_bytes,
                    canonical_plan_digest: plan_digest,
                    plan_fingerprint: [7; 32],
                    created_at: at(4),
                })
                .expect("PlanVersion draft");
            store
                .publish_plan_version(plan_version_id, None, at(5))
                .expect("publish PlanVersion");
            Self {
                _temp: temp,
                store,
                workspace_id,
                session_id,
                plan_version_id,
                plan_digest,
            }
        }

        fn submission(&self, job_number: u128, queued_at: i64) -> JobSubmission {
            JobSubmission::try_new(
                self.workspace_id,
                self.session_id,
                self.plan_version_id,
                self.plan_digest,
                Uuid::from_u128(1000 + job_number),
                format!("job-key-{job_number}"),
                Vec::new(),
                serde_json::json!({"deadlineSeconds": 900}),
                serde_json::json!({"kind": "verificationBundle"}),
                at(queued_at),
                Uuid::from_u128(10_000 + job_number),
                format!("request-{job_number}"),
                format!("correlation-{job_number}"),
                "actor:test",
            )
            .expect("valid submission")
        }

        fn claim(&self, job_id: Uuid, run_id: Uuid) -> RunRecord {
            self.store
                .claim_job(
                    job_id,
                    run_id,
                    at(20),
                    1,
                    "engine-test",
                    EventDraft::new(
                        Uuid::new_v4(),
                        EventStreamKind::Job,
                        job_id,
                        job_id,
                        None,
                        ControlPlaneEventType::JobRunning,
                        at(20),
                        "request-claim",
                        "correlation-claim",
                        "actor:test",
                        serde_json::json!({"state": "running"}),
                    ),
                    EventDraft::new(
                        Uuid::new_v4(),
                        EventStreamKind::Run,
                        run_id,
                        job_id,
                        Some(run_id),
                        ControlPlaneEventType::RunRunning,
                        at(20),
                        "request-claim",
                        "correlation-claim",
                        "actor:test",
                        serde_json::json!({"state": "running"}),
                    ),
                )
                .expect("claim")
        }
    }

    #[test]
    fn fresh_schema_and_reopen_are_idempotent() {
        let fixture = Fixture::new();
        assert_eq!(fixture.store.schema_version(), 4);
        let job = fixture
            .store
            .submit_job(fixture.submission(1, 10))
            .expect("submit");
        let job_id = match job {
            SubmitOutcome::Created(job) => job.id,
            SubmitOutcome::Replayed(_) => panic!("first submission must create"),
        };
        let root = fixture._temp.path().to_path_buf();
        drop(fixture.store);
        let reopened = ControlPlaneStore::open(root).expect("reopen");
        assert_eq!(
            reopened.get_job(job_id).expect("durable Job").state,
            JobState::Queued
        );
        assert_eq!(
            reopened
                .list_events(fixture.workspace_id, EventStreamKind::Job, job_id, None, 10)
                .expect("events")
                .events
                .len(),
            1
        );
    }

    #[test]
    fn submission_replay_and_digest_conflict_do_not_mutate() {
        let fixture = Fixture::new();
        let submission = fixture.submission(1, 10);
        let first = fixture
            .store
            .submit_job(submission.clone())
            .expect("submit");
        let first_job = match first {
            SubmitOutcome::Created(job) => job,
            SubmitOutcome::Replayed(_) => panic!("first submission must create"),
        };
        let replay = fixture.store.submit_job(submission).expect("replay");
        assert!(matches!(replay, SubmitOutcome::Replayed(_)));
        let event_count = fixture
            .store
            .list_events(
                fixture.workspace_id,
                EventStreamKind::Job,
                first_job.id,
                None,
                10,
            )
            .expect("events")
            .events
            .len();
        assert_eq!(event_count, 1);

        let mut conflict = fixture.submission(2, 11);
        conflict.idempotency_key = "job-key-1".to_owned();
        conflict.execution_policy = serde_json::json!({"deadlineSeconds": 899});
        conflict.request_digest = conflict.compute_request_digest().expect("conflict digest");
        let error = fixture
            .store
            .submit_job(conflict)
            .expect_err("digest conflict");
        assert!(matches!(error, StorageError::AlreadyExists(id) if id == first_job.id));
        assert_eq!(
            fixture
                .store
                .list_jobs(fixture.workspace_id, None, 10)
                .expect("jobs")
                .jobs
                .len(),
            1
        );
    }

    #[test]
    fn exact_queue_cap_has_zero_mutation_at_257() {
        let fixture = Fixture::new();
        for number in 0..MAX_QUEUED_JOBS_PER_WORKSPACE as u128 {
            fixture
                .store
                .submit_job(fixture.submission(number, 100 + number as i64))
                .expect("queue slot");
        }
        let rejected = fixture
            .store
            .submit_job(fixture.submission(10_000, 999))
            .expect_err("queue full");
        assert!(matches!(
            rejected,
            StorageError::Busy("control-plane queue is full")
        ));
        let page = fixture
            .store
            .list_jobs(fixture.workspace_id, None, MAX_EVENT_PAGE_SIZE)
            .expect("jobs");
        assert_eq!(page.jobs.len(), MAX_QUEUED_JOBS_PER_WORKSPACE);
        let rejected_id = Uuid::from_u128(11_000);
        assert!(
            matches!(fixture.store.get_job(rejected_id), Err(StorageError::NotFound(id)) if id == rejected_id)
        );
    }

    #[test]
    fn claim_is_atomic_and_single_run_wins() {
        let fixture = Fixture::new();
        let job = match fixture
            .store
            .submit_job(fixture.submission(1, 10))
            .expect("submit")
        {
            SubmitOutcome::Created(job) => job,
            SubmitOutcome::Replayed(_) => panic!("first submission must create"),
        };
        let run_id = Uuid::from_u128(2000);
        let run = fixture.claim(job.id, run_id);
        assert_eq!(run.state, RunState::Running);
        assert_eq!(
            fixture.store.get_job(job.id).expect("Job").state,
            JobState::Running
        );
        let second = fixture.store.claim_job(
            job.id,
            Uuid::from_u128(2001),
            at(21),
            1,
            "engine-test",
            EventDraft::new(
                Uuid::new_v4(),
                EventStreamKind::Job,
                job.id,
                job.id,
                None,
                ControlPlaneEventType::JobRunning,
                at(21),
                "r",
                "c",
                "a",
                serde_json::json!({}),
            ),
            EventDraft::new(
                Uuid::new_v4(),
                EventStreamKind::Run,
                Uuid::from_u128(2001),
                job.id,
                Some(Uuid::from_u128(2001)),
                ControlPlaneEventType::RunRunning,
                at(21),
                "r",
                "c",
                "a",
                serde_json::json!({}),
            ),
        );
        assert!(matches!(
            second,
            Err(StorageError::Busy(
                "control-plane object was already claimed"
            ))
        ));
        assert_eq!(
            fixture
                .store
                .list_runs(fixture.workspace_id, None, 10)
                .expect("Runs")
                .runs
                .len(),
            1
        );
        assert_eq!(
            fixture
                .store
                .list_events(fixture.workspace_id, EventStreamKind::Job, job.id, None, 10)
                .expect("Job events")
                .events
                .len(),
            2
        );
        assert_eq!(
            fixture
                .store
                .list_events(fixture.workspace_id, EventStreamKind::Run, run.id, None, 10)
                .expect("Run events")
                .events
                .len(),
            1
        );
    }

    #[test]
    fn terminal_job_transition_requires_matching_run_outcome() {
        let fixture = Fixture::new();
        let job = match fixture
            .store
            .submit_job(fixture.submission(1, 10))
            .expect("submit")
        {
            SubmitOutcome::Created(job) => job,
            SubmitOutcome::Replayed(_) => panic!("first submission must create"),
        };
        let run_id = Uuid::from_u128(2000);
        fixture.claim(job.id, run_id);

        let error = fixture
            .store
            .transition_job(
                job.id,
                JobState::Succeeded,
                EventDraft::new(
                    Uuid::new_v4(),
                    EventStreamKind::Job,
                    job.id,
                    job.id,
                    None,
                    ControlPlaneEventType::JobSucceeded,
                    at(21),
                    "finish",
                    "finish",
                    "actor:test",
                    serde_json::json!({"state": "succeeded"}),
                ),
                None,
            )
            .expect_err("a running Run cannot have a succeeded Job");
        assert!(matches!(
            error,
            StorageError::InvalidDraft("terminal Job state must match its terminal Run state")
        ));
        assert_eq!(
            fixture.store.get_job(job.id).expect("Job").state,
            JobState::Running
        );

        let error = fixture
            .store
            .append_event(EventDraft::new(
                Uuid::new_v4(),
                EventStreamKind::Job,
                job.id,
                job.id,
                None,
                ControlPlaneEventType::JobSucceeded,
                at(21),
                "manual",
                "manual",
                "actor:test",
                serde_json::json!({"state": "succeeded"}),
            ))
            .expect_err("lifecycle events cannot bypass a state transition");
        assert!(matches!(
            error,
            StorageError::InvalidDraft(
                "lifecycle events must be appended with their state transition"
            )
        ));
    }

    #[test]
    fn terminal_cas_and_forbidden_transition_are_immutable() {
        let fixture = Fixture::new();
        let job = match fixture
            .store
            .submit_job(fixture.submission(1, 10))
            .expect("submit")
        {
            SubmitOutcome::Created(job) => job,
            SubmitOutcome::Replayed(_) => panic!("first submission must create"),
        };
        let run_id = Uuid::from_u128(2000);
        fixture.claim(job.id, run_id);
        let (run, job) = fixture
            .store
            .finish_run_and_job(
                run_id,
                RunState::Succeeded,
                JobState::Succeeded,
                EventDraft::new(
                    Uuid::new_v4(),
                    EventStreamKind::Run,
                    run_id,
                    job.id,
                    Some(run_id),
                    ControlPlaneEventType::RunSucceeded,
                    at(30),
                    "finish",
                    "finish",
                    "actor:test",
                    serde_json::json!({"state": "succeeded"}),
                ),
                EventDraft::new(
                    Uuid::new_v4(),
                    EventStreamKind::Job,
                    job.id,
                    job.id,
                    None,
                    ControlPlaneEventType::JobSucceeded,
                    at(30),
                    "finish",
                    "finish",
                    "actor:test",
                    serde_json::json!({"state": "succeeded"}),
                ),
                None,
            )
            .expect("finish");
        assert_eq!(run.state, RunState::Succeeded);
        assert_eq!(job.state, JobState::Succeeded);
        let error = fixture
            .store
            .transition_job(
                job.id,
                JobState::Failed,
                EventDraft::new(
                    Uuid::new_v4(),
                    EventStreamKind::Job,
                    job.id,
                    job.id,
                    None,
                    ControlPlaneEventType::JobFailed,
                    at(31),
                    "late",
                    "late",
                    "actor:test",
                    serde_json::json!({}),
                ),
                Some(FailureInfo::try_new("late", false, "must not win").expect("failure")),
            )
            .expect_err("terminal Job is immutable");
        assert!(matches!(error, StorageError::InvalidDraft(_)));
        assert_eq!(
            fixture
                .store
                .list_events(fixture.workspace_id, EventStreamKind::Job, job.id, None, 10)
                .expect("events")
                .events
                .len(),
            3
        );
    }

    #[test]
    fn plan_version_digest_and_expected_current_version_are_cas_bound() {
        let fixture = Fixture::new();
        let second_id = Uuid::from_u128(5);
        let bytes = b"canonical-plan-v2".to_vec();
        let digest = sha256(&bytes);
        fixture
            .store
            .create_plan_version(PlanVersionDraft {
                workspace_id: fixture.workspace_id,
                plan_id: Uuid::from_u128(3),
                plan_version_id: second_id,
                version_number: 2,
                parent_version_id: Some(fixture.plan_version_id),
                logical_plan: serde_json::json!({"version": 2}),
                canonical_plan_bytes: bytes,
                canonical_plan_digest: digest,
                plan_fingerprint: [8; 32],
                created_at: at(40),
            })
            .expect("second PlanVersion");
        let stale = fixture
            .store
            .publish_plan_version(second_id, None, at(41))
            .expect_err("stale expected version");
        assert!(matches!(
            stale,
            StorageError::InvalidDraft("PlanVersion expected-current CAS conflict")
        ));
        let published = fixture
            .store
            .publish_plan_version(second_id, Some(fixture.plan_version_id), at(42))
            .expect("publish with exact current version");
        assert_eq!(published.state, PlanVersionState::Published);
        assert_eq!(
            fixture
                .store
                .get_plan_version(fixture.plan_version_id)
                .expect("old version")
                .state,
            PlanVersionState::Superseded
        );
    }

    #[test]
    fn artifact_reference_lifecycle_event_is_owned_by_run_stream() {
        let fixture = Fixture::new();
        let job = match fixture
            .store
            .submit_job(fixture.submission(1, 10))
            .expect("submit")
        {
            SubmitOutcome::Created(job) => job,
            SubmitOutcome::Replayed(_) => panic!("first submission must create"),
        };
        let run_id = Uuid::from_u128(2000);
        fixture.claim(job.id, run_id);
        let artifact_id = Uuid::from_u128(3000);
        fixture
            .store
            .create_artifact_ref(ArtifactRefDraft {
                workspace_id: fixture.workspace_id,
                run_id,
                artifact_id,
                artifact_kind: ArtifactKind::AcceptedSnapshot,
                external_ref_kind: ExternalRefKind::Snapshot,
                external_ref_id: Uuid::from_u128(4000),
                content_digest: [9; 32],
                metadata: serde_json::json!({"rowCount": 1}),
                created_at: at(21),
            })
            .expect("staged ArtifactRef");
        let committed = fixture
            .store
            .transition_artifact_ref(
                artifact_id,
                ArtifactRefState::Committed,
                EventDraft::new(
                    Uuid::from_u128(5000),
                    EventStreamKind::Run,
                    run_id,
                    job.id,
                    Some(run_id),
                    ControlPlaneEventType::ArtifactCommitted,
                    at(22),
                    "artifact",
                    "artifact",
                    "actor:test",
                    serde_json::json!({"state": "committed", "artifactId": artifact_id}),
                ),
            )
            .expect("commit ArtifactRef");
        assert_eq!(committed.state, ArtifactRefState::Committed);
        let run_events = fixture
            .store
            .list_events(fixture.workspace_id, EventStreamKind::Run, run_id, None, 10)
            .expect("Run events")
            .events;
        assert_eq!(run_events.len(), 2);
        assert_eq!(
            run_events[1].event_type,
            ControlPlaneEventType::ArtifactCommitted
        );
    }

    #[test]
    fn artifact_commit_is_rejected_after_run_terminal_state() {
        let fixture = Fixture::new();
        let job = match fixture
            .store
            .submit_job(fixture.submission(1, 10))
            .expect("submit")
        {
            SubmitOutcome::Created(job) => job,
            SubmitOutcome::Replayed(_) => panic!("first submission must create"),
        };
        let run_id = Uuid::from_u128(2000);
        fixture.claim(job.id, run_id);
        let artifact_id = Uuid::from_u128(3001);
        fixture
            .store
            .create_artifact_ref(ArtifactRefDraft {
                workspace_id: fixture.workspace_id,
                run_id,
                artifact_id,
                artifact_kind: ArtifactKind::AcceptedSnapshot,
                external_ref_kind: ExternalRefKind::Snapshot,
                external_ref_id: Uuid::from_u128(4001),
                content_digest: [9; 32],
                metadata: serde_json::json!({"rowCount": 1}),
                created_at: at(21),
            })
            .expect("staged ArtifactRef");
        fixture
            .store
            .finish_run_and_job(
                run_id,
                RunState::Succeeded,
                JobState::Succeeded,
                EventDraft::new(
                    Uuid::new_v4(),
                    EventStreamKind::Run,
                    run_id,
                    job.id,
                    Some(run_id),
                    ControlPlaneEventType::RunSucceeded,
                    at(30),
                    "finish",
                    "finish",
                    "actor:test",
                    serde_json::json!({"state": "succeeded"}),
                ),
                EventDraft::new(
                    Uuid::new_v4(),
                    EventStreamKind::Job,
                    job.id,
                    job.id,
                    None,
                    ControlPlaneEventType::JobSucceeded,
                    at(30),
                    "finish",
                    "finish",
                    "actor:test",
                    serde_json::json!({"state": "succeeded"}),
                ),
                None,
            )
            .expect("finish");
        let error = fixture
            .store
            .transition_artifact_ref(
                artifact_id,
                ArtifactRefState::Committed,
                EventDraft::new(
                    Uuid::new_v4(),
                    EventStreamKind::Run,
                    run_id,
                    job.id,
                    Some(run_id),
                    ControlPlaneEventType::ArtifactCommitted,
                    at(31),
                    "late-artifact",
                    "late-artifact",
                    "actor:test",
                    serde_json::json!({"state": "committed"}),
                ),
            )
            .expect_err("terminal Run cannot publish a staged ArtifactRef");
        assert!(matches!(
            error,
            StorageError::InvalidDraft("ArtifactRef commit requires a running Run")
        ));
        assert!(matches!(
            fixture.store.get_artifact_ref(artifact_id),
            Err(StorageError::NotFound(id)) if id == artifact_id
        ));
    }

    #[test]
    fn queued_cancellation_is_two_events_without_a_run() {
        let fixture = Fixture::new();
        let job = match fixture
            .store
            .submit_job(fixture.submission(1, 10))
            .expect("submit")
        {
            SubmitOutcome::Created(job) => job,
            SubmitOutcome::Replayed(_) => panic!("first submission must create"),
        };
        let cancelled = fixture
            .store
            .cancel_queued_job(
                job.id,
                EventDraft::new(
                    Uuid::new_v4(),
                    EventStreamKind::Job,
                    job.id,
                    job.id,
                    None,
                    ControlPlaneEventType::JobCancelling,
                    at(11),
                    "cancel",
                    "cancel",
                    "actor:test",
                    serde_json::json!({"state": "cancelling"}),
                ),
                EventDraft::new(
                    Uuid::new_v4(),
                    EventStreamKind::Job,
                    job.id,
                    job.id,
                    None,
                    ControlPlaneEventType::JobCancelled,
                    at(12),
                    "cancel",
                    "cancel",
                    "actor:test",
                    serde_json::json!({"state": "cancelled"}),
                ),
            )
            .expect("cancel queued Job");
        assert_eq!(cancelled.state, JobState::Cancelled);
        assert!(cancelled.run_id.is_none());
        assert_eq!(
            fixture
                .store
                .list_runs(fixture.workspace_id, None, 10)
                .expect("Runs")
                .runs
                .len(),
            0
        );
        assert_eq!(
            fixture
                .store
                .list_events(fixture.workspace_id, EventStreamKind::Job, job.id, None, 10)
                .expect("events")
                .events
                .len(),
            3
        );
    }

    #[test]
    fn event_payload_security_and_rollback_preserve_sequence() {
        let fixture = Fixture::new();
        let job = match fixture
            .store
            .submit_job(fixture.submission(1, 10))
            .expect("submit")
        {
            SubmitOutcome::Created(job) => job,
            SubmitOutcome::Replayed(_) => panic!("first submission must create"),
        };
        let unsafe_event = EventDraft::new(
            Uuid::new_v4(),
            EventStreamKind::Job,
            job.id,
            job.id,
            None,
            ControlPlaneEventType::JobCancelling,
            at(11),
            "r",
            "c",
            "a",
            serde_json::json!({"password": "secret"}),
        );
        let error = fixture
            .store
            .append_event(unsafe_event)
            .expect_err("unsafe payload");
        assert!(matches!(error, StorageError::InvalidDraft(_)));
        let oversized = "x".repeat(MAX_EVENT_PAYLOAD_BYTES);
        let oversized_event = EventDraft::new(
            Uuid::new_v4(),
            EventStreamKind::Job,
            job.id,
            job.id,
            None,
            ControlPlaneEventType::JobCancelling,
            at(12),
            "r",
            "c",
            "a",
            serde_json::json!({"label": oversized}),
        );
        assert!(matches!(
            fixture.store.append_event(oversized_event),
            Err(StorageError::InvalidDraft(_))
        ));
        let events = fixture
            .store
            .list_events(fixture.workspace_id, EventStreamKind::Job, job.id, None, 10)
            .expect("events")
            .events;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].sequence, 1);
    }

    #[test]
    fn bounded_event_pagination_rejects_foreign_cursor() {
        let fixture = Fixture::new();
        let job = match fixture
            .store
            .submit_job(fixture.submission(1, 10))
            .expect("submit")
        {
            SubmitOutcome::Created(job) => job,
            SubmitOutcome::Replayed(_) => panic!("first submission must create"),
        };
        let first_page = fixture
            .store
            .list_events(fixture.workspace_id, EventStreamKind::Job, job.id, None, 1)
            .expect("page");
        assert_eq!(first_page.events.len(), 1);
        assert!(first_page.next.is_none());
        let foreign = EventCursor {
            workspace_id: Uuid::from_u128(999),
            stream_kind: EventStreamKind::Job,
            stream_id: job.id,
            sequence: 1,
        };
        assert!(matches!(
            fixture.store.list_events(
                fixture.workspace_id,
                EventStreamKind::Job,
                job.id,
                Some(foreign),
                1
            ),
            Err(StorageError::InvalidDraft(_))
        ));
        assert!(matches!(
            fixture.store.list_events(
                fixture.workspace_id,
                EventStreamKind::Job,
                job.id,
                None,
                MAX_EVENT_PAGE_SIZE + 1
            ),
            Err(StorageError::InvalidDraft(_))
        ));
    }
}
