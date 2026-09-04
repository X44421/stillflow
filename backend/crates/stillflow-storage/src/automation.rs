//! Durable scheduler state for AUT-J1.
//!
//! The storage layer persists only trigger coordination. Job/Run lifecycle,
//! execution, and output publication remain owned by the E5 control plane and
//! JobRuntime.

use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde_json::Value;
use stillflow_core::AutomationSchedule;
use uuid::Uuid;

use crate::{
    acquire_activity, compact_json, open_connection, parse_timestamp, ActivityKind,
    ControlPlaneStore, StorageError,
};

pub const MAX_AUTOMATION_TEMPLATE_BYTES: usize = 256 * 1024;
pub const MAX_AUTOMATION_SUBMISSION_ATTEMPTS: u8 = 8;
pub const DEFAULT_AUTOMATION_CLAIM_LEASE_SECONDS: u64 = 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutomationScheduleState {
    Active,
    Paused,
    Failed,
    Deleted,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AutomationScheduleDraft {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub schedule: AutomationSchedule,
    pub timezone: String,
    pub template: Value,
    pub first_run_at: DateTime<Utc>,
    pub max_submission_attempts: u8,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AutomationTriggerLease {
    pub occurrence_key: String,
    pub claim_id: Uuid,
    pub attempt: u8,
    pub lease_expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AutomationScheduleRecord {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub schedule: AutomationSchedule,
    pub timezone: String,
    pub template: Value,
    pub state: AutomationScheduleState,
    pub first_run_at: DateTime<Utc>,
    pub next_run_at: Option<DateTime<Utc>>,
    pub last_submitted_at: Option<DateTime<Utc>>,
    pub last_occurrence_key: Option<String>,
    pub in_flight: Option<AutomationTriggerLease>,
    pub max_submission_attempts: u8,
    pub last_submission_attempt: u8,
    pub revision: u64,
    pub last_failure: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AutomationTrigger {
    pub schedule_id: Uuid,
    pub workspace_id: Uuid,
    pub schedule: AutomationSchedule,
    pub timezone: String,
    pub template: Value,
    pub occurrence_at: DateTime<Utc>,
    pub occurrence_key: String,
    pub claim_id: Uuid,
    pub attempt: u8,
    pub max_submission_attempts: u8,
}

impl ControlPlaneStore {
    pub fn create_automation_schedule(
        &self,
        draft: AutomationScheduleDraft,
    ) -> Result<AutomationScheduleRecord, StorageError> {
        validate_id(draft.id, "automation schedule")?;
        validate_id(draft.workspace_id, "automation workspace")?;
        draft
            .schedule
            .validate()
            .map_err(|_| StorageError::InvalidDraft("automation schedule is invalid"))?;
        AutomationSchedule::validate_timezone(&draft.timezone)
            .map_err(|_| StorageError::InvalidDraft("automation timezone is invalid"))?;
        if !(1..=MAX_AUTOMATION_SUBMISSION_ATTEMPTS).contains(&draft.max_submission_attempts) {
            return Err(StorageError::InvalidDraft(
                "automation submission retry bound is invalid",
            ));
        }
        crate::validate_safe_json(&draft.template, false)?;
        let template_json = compact_json(&draft.template, "serialize automation template")?;
        if template_json.len() > MAX_AUTOMATION_TEMPLATE_BYTES {
            return Err(StorageError::InvalidDraft(
                "automation template exceeds its byte bound",
            ));
        }
        if draft.first_run_at < draft.created_at {
            return Err(StorageError::InvalidTimestampOrder(
                "automation creation and first run",
            ));
        }
        let next_run_at = draft
            .schedule
            .first_at_or_after(draft.first_run_at, &draft.timezone)
            .map_err(|_| StorageError::InvalidDraft("automation first run is invalid"))?;
        let schedule_json = compact_json(&draft.schedule, "serialize automation schedule")?;
        let _activity = acquire_activity(&self.inner, ActivityKind::Publisher)?;
        let mut connection = open_connection(&self.inner)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StorageError::database("begin automation schedule creation"))?;
        ensure_workspace_active_for_automation(&transaction, draft.workspace_id)?;
        transaction
            .execute(
                "INSERT INTO aut_schedules
                 (id, workspace_id, schedule_json, timezone, template_json, state,
                  first_run_at_utc, next_run_at_utc, last_submitted_at_utc,
                  last_occurrence_key, in_flight_occurrence_key, in_flight_claim_id,
                  in_flight_attempt, in_flight_lease_expires_at_utc,
                  max_submission_attempts, revision, last_failure, created_at_utc,
                  updated_at_utc)
                 VALUES (?1, ?2, ?3, ?4, ?5, 'active', ?6, ?7, NULL, NULL, NULL,
                         NULL, NULL, NULL, ?8, 1, NULL, ?9, ?9)",
                params![
                    draft.id.to_string(),
                    draft.workspace_id.to_string(),
                    schedule_json,
                    draft.timezone,
                    template_json,
                    timestamp(&draft.first_run_at),
                    timestamp(&next_run_at),
                    i64::from(draft.max_submission_attempts),
                    timestamp(&draft.created_at),
                ],
            )
            .map_err(|error| crate::map_constraint(error, draft.id))?;
        let record = automation_schedule_from_connection(&transaction, draft.id)?;
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit automation schedule creation"))?;
        Ok(record)
    }

    pub fn get_automation_schedule(
        &self,
        schedule_id: Uuid,
    ) -> Result<AutomationScheduleRecord, StorageError> {
        validate_id(schedule_id, "automation schedule")?;
        let _activity = acquire_activity(&self.inner, ActivityKind::Reader)?;
        let connection = open_connection(&self.inner)?;
        automation_schedule_from_connection(&connection, schedule_id)
    }

    pub fn list_automation_schedules(
        &self,
        workspace_id: Uuid,
        limit: usize,
    ) -> Result<Vec<AutomationScheduleRecord>, StorageError> {
        if workspace_id.is_nil() || limit == 0 || limit > 1_024 {
            return Err(StorageError::InvalidDraft(
                "automation schedule list bound is invalid",
            ));
        }
        let _activity = acquire_activity(&self.inner, ActivityKind::Reader)?;
        let connection = open_connection(&self.inner)?;
        let mut statement = connection
            .prepare(
                "SELECT id FROM aut_schedules
                 WHERE workspace_id = ?1 ORDER BY id ASC LIMIT ?2",
            )
            .map_err(|_| StorageError::database("prepare automation schedule list"))?;
        let rows = statement
            .query_map(params![workspace_id.to_string(), limit as i64], |row| {
                row.get::<_, String>(0)
            })
            .map_err(|_| StorageError::database("read automation schedule list"))?;
        let mut schedules = Vec::with_capacity(limit);
        for row in rows {
            let id = parse_uuid(
                &row.map_err(|_| StorageError::database("decode automation schedule list"))?,
            )?;
            schedules.push(automation_schedule_from_connection(&connection, id)?);
        }
        Ok(schedules)
    }

    pub fn list_due_automation_schedule_ids(
        &self,
        workspace_id: Uuid,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<Uuid>, StorageError> {
        if workspace_id.is_nil() || limit == 0 || limit > 1_024 {
            return Err(StorageError::InvalidDraft(
                "automation due-list bound is invalid",
            ));
        }
        let _activity = acquire_activity(&self.inner, ActivityKind::Reader)?;
        let connection = open_connection(&self.inner)?;
        let mut statement = connection
            .prepare(
                "SELECT id FROM aut_schedules
                 WHERE workspace_id = ?1 AND state = 'active'
                   AND next_run_at_utc IS NOT NULL AND next_run_at_utc <= ?2
                 ORDER BY next_run_at_utc ASC, id ASC LIMIT ?3",
            )
            .map_err(|_| StorageError::database("prepare due automation schedules"))?;
        let rows = statement
            .query_map(
                params![workspace_id.to_string(), timestamp(&now), limit as i64],
                |row| row.get::<_, String>(0),
            )
            .map_err(|_| StorageError::database("read due automation schedules"))?;
        rows.map(|row| {
            let id = row.map_err(|_| StorageError::database("decode due automation schedule"))?;
            parse_uuid(&id)
        })
        .collect()
    }

    pub fn claim_due_automation_schedule(
        &self,
        schedule_id: Uuid,
        now: DateTime<Utc>,
        claim_id: Uuid,
        lease_seconds: u64,
    ) -> Result<Option<AutomationTrigger>, StorageError> {
        validate_id(schedule_id, "automation schedule")?;
        validate_id(claim_id, "automation claim")?;
        if lease_seconds == 0 || lease_seconds > 3_600 {
            return Err(StorageError::InvalidDraft(
                "automation claim lease is outside its bound",
            ));
        }
        let lease = chrono::Duration::seconds(
            i64::try_from(lease_seconds)
                .map_err(|_| StorageError::ArithmeticOverflow("automation claim lease"))?,
        );
        let lease_expires_at = now
            .checked_add_signed(lease)
            .ok_or(StorageError::ArithmeticOverflow("automation claim lease"))?;
        let _activity = acquire_activity(&self.inner, ActivityKind::Publisher)?;
        let mut connection = open_connection(&self.inner)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StorageError::database("begin automation schedule claim"))?;
        let record = automation_schedule_from_connection(&transaction, schedule_id)?;
        if record.state != AutomationScheduleState::Active
            || record.next_run_at.is_none_or(|next| next > now)
        {
            transaction
                .commit()
                .map_err(|_| StorageError::database("commit idle automation schedule claim"))?;
            return Ok(None);
        }
        let (occurrence_at, attempt) = match record.in_flight.as_ref() {
            Some(in_flight) if in_flight.lease_expires_at > now => {
                transaction
                    .commit()
                    .map_err(|_| StorageError::database("commit busy automation schedule claim"))?;
                return Ok(None);
            }
            Some(in_flight) => (
                record.next_run_at.ok_or(StorageError::Serialization(
                    "due automation has no next run",
                ))?,
                in_flight
                    .attempt
                    .checked_add(1)
                    .ok_or(StorageError::ArithmeticOverflow("automation attempt"))?,
            ),
            None => (
                record.next_run_at.ok_or(StorageError::Serialization(
                    "due automation has no next run",
                ))?,
                record
                    .last_submission_attempt
                    .checked_add(1)
                    .ok_or(StorageError::ArithmeticOverflow("automation attempt"))?,
            ),
        };
        if attempt > record.max_submission_attempts {
            return Err(StorageError::Busy(
                "automation submission attempts are exhausted",
            ));
        }
        let occurrence_key = timestamp(&occurrence_at);
        let updated_at = later(now, record.updated_at);
        let changed = transaction
            .execute(
                "UPDATE aut_schedules
                 SET in_flight_occurrence_key = ?2, in_flight_claim_id = ?3,
                     in_flight_attempt = ?4, in_flight_lease_expires_at_utc = ?5,
                     revision = revision + 1, updated_at_utc = ?6
                 WHERE id = ?1 AND revision = ?7 AND state = 'active'",
                params![
                    schedule_id.to_string(),
                    occurrence_key,
                    claim_id.to_string(),
                    i64::from(attempt),
                    timestamp(&lease_expires_at),
                    timestamp(&updated_at),
                    i64::try_from(record.revision).map_err(|_| {
                        StorageError::ArithmeticOverflow("automation schedule revision")
                    })?,
                ],
            )
            .map_err(|_| StorageError::database("claim automation schedule"))?;
        if changed != 1 {
            return Err(StorageError::Busy(
                "automation schedule changed while claiming",
            ));
        }
        let trigger = AutomationTrigger {
            schedule_id,
            workspace_id: record.workspace_id,
            schedule: record.schedule,
            timezone: record.timezone,
            template: record.template,
            occurrence_at,
            occurrence_key,
            claim_id,
            attempt,
            max_submission_attempts: record.max_submission_attempts,
        };
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit automation schedule claim"))?;
        Ok(Some(trigger))
    }

    pub fn acknowledge_automation_trigger(
        &self,
        trigger: &AutomationTrigger,
        next_run_at: DateTime<Utc>,
        submitted_at: DateTime<Utc>,
    ) -> Result<AutomationScheduleRecord, StorageError> {
        let expected_next = trigger
            .schedule
            .next_after(trigger.occurrence_at, &trigger.timezone)
            .map_err(|_| StorageError::InvalidDraft("automation next run is invalid"))?;
        if next_run_at != expected_next || next_run_at <= trigger.occurrence_at {
            return Err(StorageError::InvalidDraft(
                "automation next run does not match its schedule",
            ));
        }
        let _activity = acquire_activity(&self.inner, ActivityKind::Publisher)?;
        let mut connection = open_connection(&self.inner)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StorageError::database("begin automation trigger acknowledgement"))?;
        let record = automation_schedule_from_connection(&transaction, trigger.schedule_id)?;
        ensure_matching_claim(&record, trigger)?;
        let updated_at = later(submitted_at, record.updated_at);
        let changed = transaction
            .execute(
                "UPDATE aut_schedules
                 SET next_run_at_utc = ?2, last_submitted_at_utc = ?3,
                     last_occurrence_key = ?4, in_flight_occurrence_key = NULL,
                     in_flight_claim_id = NULL, in_flight_attempt = NULL,
                     in_flight_lease_expires_at_utc = NULL, last_failure = NULL,
                     last_submission_attempt = 0,
                     revision = revision + 1, updated_at_utc = ?5
                 WHERE id = ?1 AND revision = ?6",
                params![
                    trigger.schedule_id.to_string(),
                    timestamp(&next_run_at),
                    timestamp(&submitted_at),
                    trigger.occurrence_key,
                    timestamp(&updated_at),
                    i64::try_from(record.revision).map_err(|_| {
                        StorageError::ArithmeticOverflow("automation schedule revision")
                    })?,
                ],
            )
            .map_err(|_| StorageError::database("acknowledge automation trigger"))?;
        if changed != 1 {
            return Err(StorageError::Busy(
                "automation schedule changed while acknowledging",
            ));
        }
        let updated = automation_schedule_from_connection(&transaction, trigger.schedule_id)?;
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit automation trigger acknowledgement"))?;
        Ok(updated)
    }

    pub fn fail_automation_trigger(
        &self,
        trigger: &AutomationTrigger,
        failure: &str,
        failed_at: DateTime<Utc>,
    ) -> Result<AutomationScheduleRecord, StorageError> {
        if failure.is_empty() || failure.len() > 1_024 || has_secret_marker(failure) {
            return Err(StorageError::InvalidDraft(
                "automation failure is not safe to persist",
            ));
        }
        let _activity = acquire_activity(&self.inner, ActivityKind::Publisher)?;
        let mut connection = open_connection(&self.inner)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StorageError::database("begin automation trigger failure"))?;
        let record = automation_schedule_from_connection(&transaction, trigger.schedule_id)?;
        ensure_matching_claim(&record, trigger)?;
        let terminal = trigger.attempt >= trigger.max_submission_attempts;
        let updated_at = later(failed_at, record.updated_at);
        let changed = transaction
            .execute(
                "UPDATE aut_schedules
                 SET state = ?2,
                     next_run_at_utc = CASE WHEN ?2 = 'failed' THEN NULL ELSE next_run_at_utc END,
                     in_flight_occurrence_key = NULL, in_flight_claim_id = NULL,
                     in_flight_attempt = NULL, in_flight_lease_expires_at_utc = NULL,
                     last_failure = ?3, last_submission_attempt = ?4,
                     revision = revision + 1, updated_at_utc = ?5
                 WHERE id = ?1 AND revision = ?6",
                params![
                    trigger.schedule_id.to_string(),
                    if terminal {
                        "failed"
                    } else {
                        state_text(record.state)
                    },
                    failure,
                    i64::from(trigger.attempt),
                    timestamp(&updated_at),
                    i64::try_from(record.revision).map_err(|_| {
                        StorageError::ArithmeticOverflow("automation schedule revision")
                    })?,
                ],
            )
            .map_err(|_| StorageError::database("fail automation trigger"))?;
        if changed != 1 {
            return Err(StorageError::Busy(
                "automation schedule changed while failing",
            ));
        }
        let updated = automation_schedule_from_connection(&transaction, trigger.schedule_id)?;
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit automation trigger failure"))?;
        Ok(updated)
    }

    pub fn set_automation_schedule_state(
        &self,
        schedule_id: Uuid,
        expected_revision: u64,
        target: AutomationScheduleState,
        changed_at: DateTime<Utc>,
    ) -> Result<AutomationScheduleRecord, StorageError> {
        validate_id(schedule_id, "automation schedule")?;
        if !matches!(
            target,
            AutomationScheduleState::Active | AutomationScheduleState::Paused
        ) {
            return Err(StorageError::InvalidDraft(
                "AUT-J1 only supports pause and resume state changes",
            ));
        }
        let _activity = acquire_activity(&self.inner, ActivityKind::Publisher)?;
        let mut connection = open_connection(&self.inner)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StorageError::database("begin automation state change"))?;
        let record = automation_schedule_from_connection(&transaction, schedule_id)?;
        if !matches!(
            record.state,
            AutomationScheduleState::Active | AutomationScheduleState::Paused
        ) {
            return Err(StorageError::Busy("automation schedule is terminal"));
        }
        let changed = transaction
            .execute(
                "UPDATE aut_schedules SET state = ?2, revision = revision + 1,
                        updated_at_utc = ?3
                 WHERE id = ?1 AND revision = ?4
                   AND state IN ('active', 'paused')",
                params![
                    schedule_id.to_string(),
                    state_text(target),
                    timestamp(&later(changed_at, record.updated_at)),
                    i64::try_from(expected_revision).map_err(|_| {
                        StorageError::ArithmeticOverflow("automation schedule revision")
                    })?,
                ],
            )
            .map_err(|_| StorageError::database("set automation schedule state"))?;
        if changed != 1 {
            return Err(StorageError::Busy("automation schedule revision is stale"));
        }
        let updated = automation_schedule_from_connection(&transaction, schedule_id)?;
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit automation state change"))?;
        Ok(updated)
    }
}

fn automation_schedule_from_connection(
    connection: &Connection,
    schedule_id: Uuid,
) -> Result<AutomationScheduleRecord, StorageError> {
    let row = connection
        .query_row(
            "SELECT id, workspace_id, schedule_json, timezone, template_json, state,
                    first_run_at_utc, next_run_at_utc, last_submitted_at_utc,
                    last_occurrence_key, in_flight_occurrence_key, in_flight_claim_id,
                    in_flight_attempt, in_flight_lease_expires_at_utc,
                    max_submission_attempts, last_submission_attempt, revision, last_failure, created_at_utc,
                    updated_at_utc
             FROM aut_schedules WHERE id = ?1",
            params![schedule_id.to_string()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<String>>(8)?,
                    row.get::<_, Option<String>>(9)?,
                    row.get::<_, Option<String>>(10)?,
                    row.get::<_, Option<String>>(11)?,
                    row.get::<_, Option<i64>>(12)?,
                    row.get::<_, Option<String>>(13)?,
                    row.get::<_, i64>(14)?,
                    row.get::<_, i64>(15)?,
                    row.get::<_, i64>(16)?,
                    row.get::<_, Option<String>>(17)?,
                    row.get::<_, String>(18)?,
                    row.get::<_, String>(19)?,
                ))
            },
        )
        .optional()
        .map_err(|_| StorageError::database("read automation schedule"))?
        .ok_or(StorageError::NotFound(schedule_id))?;
    if parse_uuid(&row.0)? != schedule_id {
        return Err(StorageError::Serialization(
            "automation schedule identity mismatch",
        ));
    }
    let workspace_id = parse_uuid(&row.1)?;
    let schedule: AutomationSchedule = serde_json::from_str(&row.2)
        .map_err(|_| StorageError::Serialization("automation schedule"))?;
    schedule
        .validate()
        .map_err(|_| StorageError::Serialization("invalid persisted automation schedule"))?;
    AutomationSchedule::validate_timezone(&row.3)
        .map_err(|_| StorageError::Serialization("invalid persisted automation timezone"))?;
    let template: Value = serde_json::from_str(&row.4)
        .map_err(|_| StorageError::Serialization("automation template"))?;
    crate::validate_safe_json(&template, false)?;
    let state = parse_state(&row.5)?;
    let first_run_at = parse_timestamp(&row.6, "automation first run timestamp")?;
    let next_run_at = row
        .7
        .as_deref()
        .map(|value| parse_timestamp(value, "automation next run timestamp"))
        .transpose()?;
    let last_submitted_at = row
        .8
        .as_deref()
        .map(|value| parse_timestamp(value, "automation submitted timestamp"))
        .transpose()?;
    let in_flight = match (
        row.10.as_deref(),
        row.11.as_deref(),
        row.12,
        row.13.as_deref(),
    ) {
        (None, None, None, None) => None,
        (Some(occurrence_key), Some(claim_id), Some(attempt), Some(lease_expires)) => {
            Some(AutomationTriggerLease {
                occurrence_key: occurrence_key.to_owned(),
                claim_id: parse_uuid(claim_id)?,
                attempt: u8::try_from(attempt)
                    .map_err(|_| StorageError::Serialization("automation attempt"))?,
                lease_expires_at: parse_timestamp(
                    lease_expires,
                    "automation claim lease timestamp",
                )?,
            })
        }
        _ => return Err(StorageError::Serialization("incomplete automation claim")),
    };
    let max_submission_attempts =
        u8::try_from(row.14).map_err(|_| StorageError::Serialization("automation retry bound"))?;
    let last_submission_attempt = u8::try_from(row.15)
        .map_err(|_| StorageError::Serialization("automation submission attempt"))?;
    let revision = u64::try_from(row.16)
        .map_err(|_| StorageError::Serialization("automation schedule revision"))?;
    Ok(AutomationScheduleRecord {
        id: schedule_id,
        workspace_id,
        schedule,
        timezone: row.3,
        template,
        state,
        first_run_at,
        next_run_at,
        last_submitted_at,
        last_occurrence_key: row.9,
        in_flight,
        max_submission_attempts,
        last_submission_attempt,
        revision,
        last_failure: row.17,
        created_at: parse_timestamp(&row.18, "automation creation timestamp")?,
        updated_at: parse_timestamp(&row.19, "automation update timestamp")?,
    })
}

fn ensure_matching_claim(
    record: &AutomationScheduleRecord,
    trigger: &AutomationTrigger,
) -> Result<(), StorageError> {
    let Some(claim) = record.in_flight.as_ref() else {
        return Err(StorageError::Busy(
            "automation trigger is no longer in flight",
        ));
    };
    if record.id != trigger.schedule_id
        || record.workspace_id != trigger.workspace_id
        || claim.claim_id != trigger.claim_id
        || claim.occurrence_key != trigger.occurrence_key
        || claim.attempt != trigger.attempt
    {
        return Err(StorageError::Busy(
            "automation trigger claim does not match",
        ));
    }
    Ok(())
}

fn ensure_workspace_active_for_automation(
    transaction: &rusqlite::Transaction<'_>,
    workspace_id: Uuid,
) -> Result<(), StorageError> {
    let state: Option<String> = transaction
        .query_row(
            "SELECT state FROM cp_workspaces WHERE id = ?1",
            params![workspace_id.to_string()],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| StorageError::database("read automation workspace"))?;
    match state.as_deref() {
        Some("active") => Ok(()),
        Some(_) => Err(StorageError::Busy("automation workspace is not active")),
        None => Err(StorageError::NotFound(workspace_id)),
    }
}

fn state_text(state: AutomationScheduleState) -> &'static str {
    match state {
        AutomationScheduleState::Active => "active",
        AutomationScheduleState::Paused => "paused",
        AutomationScheduleState::Failed => "failed",
        AutomationScheduleState::Deleted => "deleted",
    }
}

fn parse_state(value: &str) -> Result<AutomationScheduleState, StorageError> {
    match value {
        "active" => Ok(AutomationScheduleState::Active),
        "paused" => Ok(AutomationScheduleState::Paused),
        "failed" => Ok(AutomationScheduleState::Failed),
        "deleted" => Ok(AutomationScheduleState::Deleted),
        _ => Err(StorageError::Serialization(
            "unknown automation schedule state",
        )),
    }
}

fn validate_id(id: Uuid, label: &'static str) -> Result<(), StorageError> {
    if id.is_nil() {
        Err(StorageError::InvalidDraft(label))
    } else {
        Ok(())
    }
}

fn parse_uuid(value: &str) -> Result<Uuid, StorageError> {
    Uuid::parse_str(value).map_err(|_| StorageError::Serialization("invalid automation UUID"))
}

fn timestamp(value: &DateTime<Utc>) -> String {
    value.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
}

fn later(left: DateTime<Utc>, right: DateTime<Utc>) -> DateTime<Utc> {
    left.max(right)
}

fn has_secret_marker(value: &str) -> bool {
    let value = value.to_ascii_lowercase();
    ["password=", "token=", "api_key=", "secret=", "bearer "]
        .iter()
        .any(|marker| value.contains(marker))
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

    fn draft(workspace_id: Uuid, schedule_id: Uuid, max_attempts: u8) -> AutomationScheduleDraft {
        AutomationScheduleDraft {
            id: schedule_id,
            workspace_id,
            schedule: AutomationSchedule::Interval { period_seconds: 60 },
            timezone: "UTC".to_owned(),
            template: json!({"planVersionId": "plan-v1"}),
            first_run_at: at(100),
            max_submission_attempts: max_attempts,
            created_at: at(100),
        }
    }

    fn store(temp: &TempDir, workspace_id: Uuid) -> ControlPlaneStore {
        let store = ControlPlaneStore::open(temp.path()).expect("open control plane");
        store
            .create_workspace(workspace_id, at(1))
            .expect("create workspace");
        store
    }

    #[test]
    fn claim_is_durable_idempotent_and_pause_is_cas_guarded() {
        let temp = tempfile::tempdir().expect("temp root");
        let workspace_id = Uuid::from_u128(1);
        let schedule_id = Uuid::from_u128(2);
        let store = store(&temp, workspace_id);
        let created = store
            .create_automation_schedule(draft(workspace_id, schedule_id, 3))
            .expect("create schedule");
        let first = store
            .claim_due_automation_schedule(schedule_id, at(100), Uuid::from_u128(3), 10)
            .expect("claim")
            .expect("due trigger");
        assert_eq!(first.occurrence_key, timestamp(&at(100)));
        assert_eq!(first.attempt, 1);
        assert!(store
            .claim_due_automation_schedule(schedule_id, at(100), Uuid::from_u128(4), 10)
            .expect("duplicate claim")
            .is_none());

        let paused = store
            .set_automation_schedule_state(
                schedule_id,
                created.revision + 1,
                AutomationScheduleState::Paused,
                at(105),
            )
            .expect("pause");
        assert_eq!(paused.state, AutomationScheduleState::Paused);
        let stale = store.set_automation_schedule_state(
            schedule_id,
            created.revision,
            AutomationScheduleState::Active,
            at(106),
        );
        assert!(matches!(stale, Err(StorageError::Busy(_))));
        drop(store);

        let reopened = ControlPlaneStore::open(temp.path()).expect("reopen control plane");
        assert!(reopened
            .claim_due_automation_schedule(schedule_id, at(110), Uuid::from_u128(5), 10)
            .expect("paused claim")
            .is_none());
        let resumed = reopened
            .set_automation_schedule_state(
                schedule_id,
                paused.revision,
                AutomationScheduleState::Active,
                at(111),
            )
            .expect("resume");
        let second = reopened
            .claim_due_automation_schedule(schedule_id, at(111), Uuid::from_u128(6), 10)
            .expect("expired claim")
            .expect("recovered trigger");
        assert_eq!(second.occurrence_at, first.occurrence_at);
        assert_eq!(second.attempt, 2);
        assert_ne!(second.claim_id, first.claim_id);
        let acknowledged = reopened
            .acknowledge_automation_trigger(&second, at(160), at(112))
            .expect("acknowledge");
        assert_eq!(acknowledged.state, AutomationScheduleState::Active);
        assert_eq!(acknowledged.next_run_at, Some(at(160)));
        assert_eq!(acknowledged.last_occurrence_key, Some(first.occurrence_key));
        assert_eq!(resumed.revision + 2, acknowledged.revision);
    }

    #[test]
    fn retry_exhaustion_is_terminal_and_bounded() {
        let temp = tempfile::tempdir().expect("temp root");
        let workspace_id = Uuid::from_u128(10);
        let schedule_id = Uuid::from_u128(11);
        let store = store(&temp, workspace_id);
        store
            .create_automation_schedule(draft(workspace_id, schedule_id, 2))
            .expect("create schedule");
        let first = store
            .claim_due_automation_schedule(schedule_id, at(100), Uuid::from_u128(12), 10)
            .expect("first claim")
            .expect("first trigger");
        let retrying = store
            .fail_automation_trigger(&first, "E5 submission unavailable", at(101))
            .expect("bounded failure");
        assert_eq!(retrying.state, AutomationScheduleState::Active);
        assert_eq!(retrying.last_submission_attempt, 1);
        let second = store
            .claim_due_automation_schedule(schedule_id, at(102), Uuid::from_u128(13), 10)
            .expect("second claim")
            .expect("second trigger");
        assert_eq!(second.attempt, 2);
        let failed = store
            .fail_automation_trigger(&second, "E5 submission unavailable", at(103))
            .expect("terminal failure");
        assert_eq!(failed.state, AutomationScheduleState::Failed);
        assert_eq!(failed.next_run_at, None);
        assert!(store
            .list_due_automation_schedule_ids(workspace_id, at(10_000), 10)
            .expect("due schedules")
            .is_empty());
    }
}
