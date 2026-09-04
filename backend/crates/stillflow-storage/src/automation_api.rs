//! AUT-A1 durable Automation API projection.
//!
//! This module adds only definition CAS operations and a bounded execution
//! handoff/history projection. Job and Run lifecycle remain owned by E5.

use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde_json::Value;
use stillflow_core::AutomationSchedule;
use uuid::Uuid;

use crate::{
    acquire_activity, compact_json, format_timestamp, open_connection, parse_timestamp,
    validate_safe_json, ActivityKind, AutomationScheduleRecord, AutomationScheduleState,
    ControlPlaneStore, StorageError, MAX_AUTOMATION_TEMPLATE_BYTES,
};

pub const MAX_AUTOMATION_NAME_BYTES: usize = 128;
pub const MAX_AUTOMATION_HISTORY_PAGE_SIZE: usize = 100;
pub const MAX_AUTOMATION_TRIGGER_KEY_BYTES: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutomationScheduleCursor {
    pub created_at: DateTime<Utc>,
    pub execution_id: Uuid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutomationExecutionState {
    Accepted,
    Submitted,
    Failed,
    Skipped,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutomationExecutionDraft {
    pub execution_id: Uuid,
    pub workspace_id: Uuid,
    pub schedule_id: Uuid,
    pub trigger_kind: String,
    pub occurrence_key: String,
    pub idempotency_key: String,
    pub request_digest: [u8; 32],
    pub job_id: Uuid,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutomationExecutionRecord {
    pub execution_id: Uuid,
    pub workspace_id: Uuid,
    pub schedule_id: Uuid,
    pub trigger_kind: String,
    pub occurrence_key: String,
    pub idempotency_key: String,
    pub request_digest: [u8; 32],
    pub job_id: Uuid,
    pub state: AutomationExecutionState,
    pub failure: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutomationExecutionCursor {
    pub created_at: DateTime<Utc>,
    pub execution_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AutomationExecutionCreateOutcome {
    Created(AutomationExecutionRecord),
    Replayed(AutomationExecutionRecord),
}

impl ControlPlaneStore {
    pub fn update_automation_schedule(
        &self,
        schedule_id: Uuid,
        expected_revision: u64,
        schedule: AutomationSchedule,
        timezone: &str,
        template: Value,
        changed_at: DateTime<Utc>,
    ) -> Result<AutomationScheduleRecord, StorageError> {
        schedule
            .validate()
            .map_err(|_| StorageError::InvalidDraft("automation schedule is invalid"))?;
        AutomationSchedule::validate_timezone(timezone)
            .map_err(|_| StorageError::InvalidDraft("automation timezone is invalid"))?;
        validate_template(&template)?;
        let current = self.get_automation_schedule(schedule_id)?;
        if !matches!(
            current.state,
            AutomationScheduleState::Active | AutomationScheduleState::Paused
        ) {
            return Err(StorageError::Busy("automation schedule is terminal"));
        }
        if current.revision != expected_revision {
            return Err(StorageError::Busy("automation schedule revision is stale"));
        }
        let effective_changed_at = later(changed_at, current.updated_at);
        let next_run_at = schedule
            .first_at_or_after(effective_changed_at, timezone)
            .map_err(|_| StorageError::InvalidDraft("automation next run is invalid"))?;
        let schedule_json = compact_json(&schedule, "serialize automation schedule")?;
        let template_json = compact_json(&template, "serialize automation template")?;
        let _activity = acquire_activity(&self.inner, ActivityKind::Publisher)?;
        let mut connection = open_connection(&self.inner)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StorageError::database("begin automation schedule update"))?;
        let changed = transaction
            .execute(
                "UPDATE aut_schedules
                 SET schedule_json = ?2, timezone = ?3, template_json = ?4,
                     next_run_at_utc = ?5, revision = revision + 1,
                     updated_at_utc = ?6
                 WHERE id = ?1 AND revision = ?7
                   AND state IN ('active', 'paused')",
                params![
                    schedule_id.to_string(),
                    schedule_json,
                    timezone,
                    template_json,
                    format_timestamp(&next_run_at),
                    format_timestamp(&effective_changed_at),
                    i64::try_from(expected_revision)
                        .map_err(|_| StorageError::ArithmeticOverflow("automation revision"))?,
                ],
            )
            .map_err(|_| StorageError::database("update automation schedule"))?;
        if changed != 1 {
            return Err(StorageError::Busy(
                "automation schedule changed while updating",
            ));
        }
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit automation schedule update"))?;
        self.get_automation_schedule(schedule_id)
    }

    pub fn delete_automation_schedule(
        &self,
        schedule_id: Uuid,
        expected_revision: u64,
        deleted_at: DateTime<Utc>,
    ) -> Result<AutomationScheduleRecord, StorageError> {
        let current = self.get_automation_schedule(schedule_id)?;
        if current.in_flight.is_some() {
            return Err(StorageError::Busy(
                "automation schedule has an in-flight trigger",
            ));
        }
        if current.revision != expected_revision {
            return Err(StorageError::Busy("automation schedule revision is stale"));
        }
        let _activity = acquire_activity(&self.inner, ActivityKind::Publisher)?;
        let mut connection = open_connection(&self.inner)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StorageError::database("begin automation schedule deletion"))?;
        let changed = transaction
            .execute(
                "UPDATE aut_schedules
                 SET state = 'deleted', next_run_at_utc = NULL,
                     revision = revision + 1, updated_at_utc = ?2
                 WHERE id = ?1 AND revision = ?3
                   AND state IN ('active', 'paused', 'failed')",
                params![
                    schedule_id.to_string(),
                    format_timestamp(&later(deleted_at, current.updated_at)),
                    i64::try_from(expected_revision)
                        .map_err(|_| StorageError::ArithmeticOverflow("automation revision"))?,
                ],
            )
            .map_err(|_| StorageError::database("delete automation schedule"))?;
        if changed != 1 {
            return Err(StorageError::Busy(
                "automation schedule changed while deleting",
            ));
        }
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit automation schedule deletion"))?;
        self.get_automation_schedule(schedule_id)
    }

    pub fn list_automation_schedules_page(
        &self,
        workspace_id: Uuid,
        cursor: Option<AutomationScheduleCursor>,
        limit: usize,
    ) -> Result<Vec<AutomationScheduleRecord>, StorageError> {
        if workspace_id.is_nil() || limit == 0 || limit > MAX_AUTOMATION_HISTORY_PAGE_SIZE {
            return Err(StorageError::InvalidDraft(
                "automation list page bound is invalid",
            ));
        }
        let _activity = acquire_activity(&self.inner, ActivityKind::Reader)?;
        let connection = open_connection(&self.inner)?;
        let mut statement = connection
            .prepare(
                "SELECT id FROM aut_schedules
                 WHERE workspace_id = ?1
                   AND (?2 IS NULL OR created_at_utc > ?2
                        OR (created_at_utc = ?2 AND id > ?3))
                 ORDER BY created_at_utc ASC, id ASC LIMIT ?4",
            )
            .map_err(|_| StorageError::database("prepare automation list page"))?;
        let rows = statement
            .query_map(
                params![
                    workspace_id.to_string(),
                    cursor
                        .as_ref()
                        .map(|value| format_timestamp(&value.created_at)),
                    cursor.as_ref().map(|value| value.execution_id.to_string()),
                    limit as i64,
                ],
                |row| row.get::<_, String>(0),
            )
            .map_err(|_| StorageError::database("read automation list page"))?;
        let ids = rows
            .map(|row| row.map_err(|_| StorageError::database("decode automation list page")))
            .collect::<Result<Vec<_>, _>>()?;
        ids.into_iter()
            .map(|id| {
                let id = Uuid::parse_str(&id)
                    .map_err(|_| StorageError::Serialization("automation UUID"))?;
                let record = self.get_automation_schedule(id)?;
                if record.workspace_id != workspace_id {
                    return Err(StorageError::Serialization("automation workspace mismatch"));
                }
                Ok(record)
            })
            .collect()
    }

    pub fn create_automation_execution(
        &self,
        draft: AutomationExecutionDraft,
    ) -> Result<AutomationExecutionCreateOutcome, StorageError> {
        validate_execution_draft(&draft)?;
        let _activity = acquire_activity(&self.inner, ActivityKind::Publisher)?;
        let mut connection = open_connection(&self.inner)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StorageError::database("begin automation execution creation"))?;
        let schedule_workspace: Option<String> = transaction
            .query_row(
                "SELECT workspace_id FROM aut_schedules WHERE id = ?1",
                [draft.schedule_id.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(|_| StorageError::database("read automation execution schedule"))?;
        let Some(schedule_workspace) = schedule_workspace else {
            return Err(StorageError::NotFound(draft.schedule_id));
        };
        if schedule_workspace != draft.workspace_id.to_string() {
            return Err(StorageError::NotFound(draft.schedule_id));
        }
        let existing_by_identity = find_execution_by_identity(&transaction, &draft)?;
        let existing_by_key = transaction
            .query_row(
                "SELECT execution_id FROM aut_executions
                 WHERE workspace_id = ?1 AND idempotency_key = ?2",
                params![draft.workspace_id.to_string(), draft.idempotency_key],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|_| StorageError::database("read automation idempotency record"))?;
        let existing = match (existing_by_identity, existing_by_key) {
            (Some(identity), Some(key)) if identity.execution_id.to_string() != key => {
                return Err(StorageError::InvalidDraft(
                    "automation idempotency identity conflicts",
                ));
            }
            (Some(identity), _) => Some(identity),
            (None, Some(key)) => Some(execution_from_connection(&transaction, &key)?),
            (None, None) => None,
        };
        if let Some(existing) = existing {
            if existing.request_digest != draft.request_digest {
                return Err(StorageError::InvalidDraft(
                    "automation idempotency key was reused with a different request",
                ));
            }
            transaction
                .commit()
                .map_err(|_| StorageError::database("commit automation execution replay"))?;
            return Ok(AutomationExecutionCreateOutcome::Replayed(existing));
        }
        transaction
            .execute(
                "INSERT INTO aut_executions
                 (execution_id, workspace_id, schedule_id, trigger_kind,
                  occurrence_key, idempotency_key, request_digest, job_id,
                  state, failure, created_at_utc, updated_at_utc)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'accepted', NULL, ?9, ?9)",
                params![
                    draft.execution_id.to_string(),
                    draft.workspace_id.to_string(),
                    draft.schedule_id.to_string(),
                    draft.trigger_kind,
                    draft.occurrence_key,
                    draft.idempotency_key,
                    hex_digest(&draft.request_digest),
                    draft.job_id.to_string(),
                    format_timestamp(&draft.created_at),
                ],
            )
            .map_err(|error| crate::map_constraint(error, draft.execution_id))?;
        let record = execution_from_connection(&transaction, &draft.execution_id.to_string())?;
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit automation execution creation"))?;
        Ok(AutomationExecutionCreateOutcome::Created(record))
    }

    pub fn mark_automation_execution_submitted(
        &self,
        execution_id: Uuid,
        job_id: Uuid,
        submitted_at: DateTime<Utc>,
    ) -> Result<AutomationExecutionRecord, StorageError> {
        if execution_id.is_nil() || job_id.is_nil() {
            return Err(StorageError::InvalidDraft("automation execution identity"));
        }
        let _activity = acquire_activity(&self.inner, ActivityKind::Publisher)?;
        let mut connection = open_connection(&self.inner)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StorageError::database("begin automation execution submission"))?;
        let existing = execution_from_connection(&transaction, &execution_id.to_string())?;
        if existing.job_id != job_id {
            return Err(StorageError::Busy(
                "automation execution Job identity changed",
            ));
        }
        if existing.state == AutomationExecutionState::Submitted {
            transaction
                .commit()
                .map_err(|_| StorageError::database("commit automation execution replay"))?;
            return Ok(existing);
        }
        let changed = transaction
            .execute(
                "UPDATE aut_executions SET state = 'submitted', failure = NULL,
                        updated_at_utc = ?2
                 WHERE execution_id = ?1 AND job_id = ?3 AND state = 'accepted'",
                params![
                    execution_id.to_string(),
                    format_timestamp(&submitted_at),
                    job_id.to_string(),
                ],
            )
            .map_err(|_| StorageError::database("mark automation execution submitted"))?;
        if changed != 1 {
            return Err(StorageError::Busy(
                "automation execution changed while submitting",
            ));
        }
        let updated = execution_from_connection(&transaction, &execution_id.to_string())?;
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit automation execution submission"))?;
        Ok(updated)
    }

    pub fn get_automation_execution(
        &self,
        execution_id: Uuid,
    ) -> Result<AutomationExecutionRecord, StorageError> {
        if execution_id.is_nil() {
            return Err(StorageError::InvalidDraft("automation execution identity"));
        }
        let _activity = acquire_activity(&self.inner, ActivityKind::Reader)?;
        let connection = open_connection(&self.inner)?;
        execution_from_connection(&connection, &execution_id.to_string())
    }

    pub fn list_automation_executions(
        &self,
        workspace_id: Uuid,
        schedule_id: Uuid,
        cursor: Option<AutomationExecutionCursor>,
        limit: usize,
    ) -> Result<Vec<AutomationExecutionRecord>, StorageError> {
        if workspace_id.is_nil()
            || schedule_id.is_nil()
            || limit == 0
            || limit > MAX_AUTOMATION_HISTORY_PAGE_SIZE
        {
            return Err(StorageError::InvalidDraft(
                "automation history page bound is invalid",
            ));
        }
        let _activity = acquire_activity(&self.inner, ActivityKind::Reader)?;
        let connection = open_connection(&self.inner)?;
        let mut statement = connection
            .prepare(
                "SELECT execution_id FROM aut_executions
                 WHERE workspace_id = ?1 AND schedule_id = ?2
                   AND (?3 IS NULL OR created_at_utc < ?3
                        OR (created_at_utc = ?3 AND execution_id < ?4))
                 ORDER BY created_at_utc DESC, execution_id DESC LIMIT ?5",
            )
            .map_err(|_| StorageError::database("prepare automation history"))?;
        let rows = statement
            .query_map(
                params![
                    workspace_id.to_string(),
                    schedule_id.to_string(),
                    cursor
                        .as_ref()
                        .map(|value| format_timestamp(&value.created_at)),
                    cursor.as_ref().map(|value| value.execution_id.to_string()),
                    limit as i64,
                ],
                |row| row.get::<_, String>(0),
            )
            .map_err(|_| StorageError::database("read automation history"))?;
        rows.map(|row| {
            let id = row.map_err(|_| StorageError::database("decode automation history"))?;
            let record = execution_from_connection(&connection, &id)?;
            if record.workspace_id != workspace_id || record.schedule_id != schedule_id {
                return Err(StorageError::Serialization(
                    "automation history scope mismatch",
                ));
            }
            Ok(record)
        })
        .collect()
    }
}

fn validate_execution_draft(draft: &AutomationExecutionDraft) -> Result<(), StorageError> {
    if draft.execution_id.is_nil()
        || draft.workspace_id.is_nil()
        || draft.schedule_id.is_nil()
        || draft.job_id.is_nil()
    {
        return Err(StorageError::InvalidDraft("automation execution identity"));
    }
    validate_text(&draft.trigger_kind, 32, "automation trigger kind")?;
    validate_text(
        &draft.occurrence_key,
        MAX_AUTOMATION_TRIGGER_KEY_BYTES,
        "automation occurrence key",
    )?;
    validate_text(
        &draft.idempotency_key,
        MAX_AUTOMATION_TRIGGER_KEY_BYTES,
        "automation idempotency key",
    )?;
    Ok(())
}

fn validate_template(template: &Value) -> Result<(), StorageError> {
    validate_safe_json(template, false)?;
    let bytes = compact_json(template, "serialize automation template")?;
    if bytes.len() > MAX_AUTOMATION_TEMPLATE_BYTES {
        return Err(StorageError::InvalidDraft(
            "automation template exceeds its byte bound",
        ));
    }
    Ok(())
}

fn validate_text(value: &str, max_bytes: usize, label: &'static str) -> Result<(), StorageError> {
    if value.is_empty()
        || value.len() > max_bytes
        || value.trim() != value
        || value.chars().any(char::is_control)
        || has_secret_marker(value)
    {
        return Err(StorageError::InvalidDraft(label));
    }
    Ok(())
}

fn has_secret_marker(value: &str) -> bool {
    let value = value.to_ascii_lowercase();
    ["password=", "token=", "api_key=", "secret=", "bearer "]
        .iter()
        .any(|marker| value.contains(marker))
}

fn later(left: DateTime<Utc>, right: DateTime<Utc>) -> DateTime<Utc> {
    left.max(right)
}

fn parse_state(value: &str) -> Result<AutomationExecutionState, StorageError> {
    match value {
        "accepted" => Ok(AutomationExecutionState::Accepted),
        "submitted" => Ok(AutomationExecutionState::Submitted),
        "failed" => Ok(AutomationExecutionState::Failed),
        "skipped" => Ok(AutomationExecutionState::Skipped),
        _ => Err(StorageError::Serialization("automation execution state")),
    }
}

fn find_execution_by_identity(
    connection: &Connection,
    draft: &AutomationExecutionDraft,
) -> Result<Option<AutomationExecutionRecord>, StorageError> {
    let id = connection
        .query_row(
            "SELECT execution_id FROM aut_executions
             WHERE workspace_id = ?1 AND schedule_id = ?2
               AND trigger_kind = ?3 AND occurrence_key = ?4",
            params![
                draft.workspace_id.to_string(),
                draft.schedule_id.to_string(),
                draft.trigger_kind,
                draft.occurrence_key,
            ],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|_| StorageError::database("read automation execution identity"))?;
    id.map(|value| execution_from_connection(connection, &value))
        .transpose()
}

fn execution_from_connection(
    connection: &Connection,
    execution_id: &str,
) -> Result<AutomationExecutionRecord, StorageError> {
    let row = connection
        .query_row(
            "SELECT execution_id, workspace_id, schedule_id, trigger_kind,
                    occurrence_key, idempotency_key, request_digest, job_id,
                    state, failure, created_at_utc, updated_at_utc
             FROM aut_executions WHERE execution_id = ?1",
            [execution_id],
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
                    row.get::<_, Option<String>>(9)?,
                    row.get::<_, String>(10)?,
                    row.get::<_, String>(11)?,
                ))
            },
        )
        .optional()
        .map_err(|_| StorageError::database("read automation execution"))?
        .ok_or_else(|| {
            Uuid::parse_str(execution_id)
                .map(StorageError::NotFound)
                .unwrap_or(StorageError::Serialization("automation execution UUID"))
        })?;
    Ok(AutomationExecutionRecord {
        execution_id: parse_uuid(&row.0)?,
        workspace_id: parse_uuid(&row.1)?,
        schedule_id: parse_uuid(&row.2)?,
        trigger_kind: row.3,
        occurrence_key: row.4,
        idempotency_key: row.5,
        request_digest: parse_digest(&row.6)?,
        job_id: parse_uuid(&row.7)?,
        state: parse_state(&row.8)?,
        failure: row.9,
        created_at: parse_timestamp(&row.10, "automation execution creation")?,
        updated_at: parse_timestamp(&row.11, "automation execution update")?,
    })
}

fn parse_uuid(value: &str) -> Result<Uuid, StorageError> {
    Uuid::parse_str(value).map_err(|_| StorageError::Serialization("automation UUID"))
}

fn parse_digest(value: &str) -> Result<[u8; 32], StorageError> {
    if value.len() != 64 {
        return Err(StorageError::Serialization("automation request digest"));
    }
    let mut digest = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = hex_nibble(pair[0]).ok_or(StorageError::Serialization("automation digest"))?;
        let low = hex_nibble(pair[1]).ok_or(StorageError::Serialization("automation digest"))?;
        digest[index] = (high << 4) | low;
    }
    Ok(digest)
}

fn hex_digest(value: &[u8; 32]) -> String {
    let mut digest = String::with_capacity(64);
    for byte in value {
        digest.push_str(&format!("{byte:02x}"));
    }
    digest
}

fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;
    use tempfile::TempDir;

    fn at(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0).single().expect("timestamp")
    }

    fn store(temp: &TempDir, workspace_id: Uuid, schedule_id: Uuid) -> ControlPlaneStore {
        let store = ControlPlaneStore::open(temp.path()).expect("open control plane");
        store
            .create_workspace(workspace_id, at(1))
            .expect("create workspace");
        store
            .create_automation_schedule(crate::AutomationScheduleDraft {
                id: schedule_id,
                workspace_id,
                schedule: AutomationSchedule::Interval { period_seconds: 60 },
                timezone: "UTC".to_owned(),
                template: json!({"name": "daily", "runTemplate": {"planVersionId": "opaque"}}),
                first_run_at: at(100),
                max_submission_attempts: 3,
                created_at: at(1),
            })
            .expect("create schedule");
        store
    }

    #[test]
    fn automation_update_and_execution_replay_are_bounded_and_cas_guarded() {
        let temp = tempfile::tempdir().expect("temporary storage root");
        let workspace_id = Uuid::from_u128(1);
        let schedule_id = Uuid::from_u128(2);
        let store = store(&temp, workspace_id, schedule_id);
        let updated = store
            .update_automation_schedule(
                schedule_id,
                1,
                AutomationSchedule::Interval {
                    period_seconds: 120,
                },
                "UTC",
                json!({"name": "updated", "runTemplate": {"planVersionId": "opaque"}}),
                at(5),
            )
            .expect("update schedule");
        assert_eq!(updated.revision, 2);
        assert!(matches!(
            store.update_automation_schedule(
                schedule_id,
                1,
                AutomationSchedule::Interval {
                    period_seconds: 120
                },
                "UTC",
                json!({"name": "stale", "runTemplate": {"planVersionId": "opaque"}}),
                at(6),
            ),
            Err(StorageError::Busy(_))
        ));

        let digest = [7_u8; 32];
        let first = store
            .create_automation_execution(AutomationExecutionDraft {
                execution_id: Uuid::from_u128(3),
                workspace_id,
                schedule_id,
                trigger_kind: "manual".to_owned(),
                occurrence_key: "manual:one".to_owned(),
                idempotency_key: "request-one".to_owned(),
                request_digest: digest,
                job_id: Uuid::from_u128(4),
                created_at: at(10),
            })
            .expect("create execution");
        assert!(matches!(
            first,
            AutomationExecutionCreateOutcome::Created(_)
        ));
        let replay = store
            .create_automation_execution(AutomationExecutionDraft {
                execution_id: Uuid::from_u128(5),
                workspace_id,
                schedule_id,
                trigger_kind: "manual".to_owned(),
                occurrence_key: "manual:one".to_owned(),
                idempotency_key: "request-one".to_owned(),
                request_digest: digest,
                job_id: Uuid::from_u128(6),
                created_at: at(11),
            })
            .expect("replay execution");
        let replay = match replay {
            AutomationExecutionCreateOutcome::Replayed(record) => record,
            AutomationExecutionCreateOutcome::Created(_) => panic!("expected replay"),
        };
        assert_eq!(replay.execution_id, Uuid::from_u128(3));
        let submitted = store
            .mark_automation_execution_submitted(replay.execution_id, replay.job_id, at(12))
            .expect("mark submitted");
        assert_eq!(submitted.state, AutomationExecutionState::Submitted);
        let history = store
            .list_automation_executions(workspace_id, schedule_id, None, 1)
            .expect("list history");
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].job_id, Uuid::from_u128(4));
    }
}
