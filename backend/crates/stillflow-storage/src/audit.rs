//! Durable AUD-C0 audit records and bounded query support.
//!
//! Audit events are intentionally separate from the operational `cp_events`
//! stream. This adapter owns append-only identity, workspace sequencing,
//! idempotent replay, redaction-safe storage, and filter-bound cursors. It
//! does not infer or emit Job/Run lifecycle events.

use std::fmt;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use rusqlite::{
    params, params_from_iter, types::ToSql, OptionalExtension, Row, Transaction,
    TransactionBehavior,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    acquire_activity, format_timestamp, open_connection, parse_timestamp, validate_safe_json,
    ActivityKind, StorageError, StoreInner,
};

pub const AUDIT_VERSION: u16 = 1;
pub const MAX_AUDIT_PAGE_SIZE: usize = stillflow_core::MAX_EVENT_PAGE_SIZE;
pub const MAX_AUDIT_LINEAGE_EDGES: usize = 128;
pub const MAX_AUDIT_TEXT_BYTES: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AuditActorKind {
    User,
    ServiceAccount,
    System,
}

impl AuditActorKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::ServiceAccount => "service_account",
            Self::System => "system",
        }
    }

    fn parse(value: &str) -> Result<Self, StorageError> {
        match value {
            "user" => Ok(Self::User),
            "service_account" => Ok(Self::ServiceAccount),
            "system" => Ok(Self::System),
            _ => Err(StorageError::InvalidDraft("unknown audit actor kind")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditActor {
    pub kind: AuditActorKind,
    pub actor_ref: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditObjectRef {
    pub kind: String,
    pub id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditLineageEdge {
    pub relation: String,
    pub from: AuditObjectRef,
    pub to: AuditObjectRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AuditRetentionState {
    Active,
    Retained,
    Expired,
}

impl AuditRetentionState {
    fn parse(value: &str) -> Result<Self, StorageError> {
        match value {
            "active" => Ok(Self::Active),
            "retained" => Ok(Self::Retained),
            "expired" => Ok(Self::Expired),
            _ => Err(StorageError::InvalidDraft("unknown audit retention state")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditEventDraft {
    pub event_id: Uuid,
    pub audit_version: u16,
    pub workspace_id: Uuid,
    pub occurred_at: DateTime<Utc>,
    pub actor: AuditActor,
    pub action: String,
    pub reason_code: String,
    pub request_id: String,
    pub correlation_id: Option<String>,
    pub trace_id: Option<String>,
    pub object: AuditObjectRef,
    pub before: Option<Value>,
    pub after: Option<Value>,
    pub lineage: Vec<AuditLineageEdge>,
    pub source_event_id: Option<Uuid>,
    pub payload: Value,
    pub idempotency_key: Option<String>,
}

impl AuditEventDraft {
    fn validate(&self) -> Result<(), StorageError> {
        if self.event_id.is_nil() || self.workspace_id.is_nil() || self.object.id.is_nil() {
            return Err(StorageError::InvalidDraft(
                "audit identities must not be nil",
            ));
        }
        if self.audit_version != AUDIT_VERSION {
            return Err(StorageError::InvalidDraft(
                "unsupported audit event version",
            ));
        }
        validate_text(&self.actor.actor_ref, "audit actor reference")?;
        validate_text(&self.action, "audit action")?;
        validate_text(&self.reason_code, "audit reason code")?;
        validate_text(&self.request_id, "audit request identity")?;
        validate_optional_text(self.correlation_id.as_deref(), "audit correlation identity")?;
        validate_optional_text(self.trace_id.as_deref(), "audit trace identity")?;
        validate_text(&self.object.kind, "audit object kind")?;
        if let Some(event_id) = self.source_event_id {
            if event_id.is_nil() {
                return Err(StorageError::InvalidDraft(
                    "source audit event must not be nil",
                ));
            }
        }
        validate_optional_text(self.idempotency_key.as_deref(), "audit idempotency key")?;
        if self
            .idempotency_key
            .as_ref()
            .is_some_and(|value| value.len() > 128)
        {
            return Err(StorageError::InvalidDraft(
                "audit idempotency key exceeds 128 bytes",
            ));
        }
        if self.lineage.len() > MAX_AUDIT_LINEAGE_EDGES {
            return Err(StorageError::InvalidDraft(
                "audit lineage exceeds the supported bound",
            ));
        }
        for edge in &self.lineage {
            validate_text(&edge.relation, "audit lineage relation")?;
            for object in [&edge.from, &edge.to] {
                if object.id.is_nil() {
                    return Err(StorageError::InvalidDraft(
                        "audit lineage identity must not be nil",
                    ));
                }
                validate_text(&object.kind, "audit lineage object kind")?;
            }
        }
        for value in [
            self.before.as_ref(),
            self.after.as_ref(),
            Some(&self.payload),
        ]
        .into_iter()
        .flatten()
        {
            validate_safe_json(value, true)?;
            if serde_json::to_vec(value)
                .map_err(|_| StorageError::Serialization("serialize audit JSON"))?
                .len()
                > stillflow_core::MAX_EVENT_PAYLOAD_BYTES
            {
                return Err(StorageError::InvalidDraft(
                    "audit JSON field exceeds the 64 KiB bound",
                ));
            }
        }
        Ok(())
    }

    fn digest(&self) -> Result<[u8; 32], StorageError> {
        let input = AuditDigestInput {
            event_id: self.event_id,
            audit_version: self.audit_version,
            workspace_id: self.workspace_id,
            occurred_at: self.occurred_at,
            actor: &self.actor,
            action: &self.action,
            reason_code: &self.reason_code,
            request_id: &self.request_id,
            correlation_id: &self.correlation_id,
            trace_id: &self.trace_id,
            object: &self.object,
            before: &self.before,
            after: &self.after,
            lineage: &self.lineage,
            source_event_id: &self.source_event_id,
            payload: &self.payload,
        };
        let bytes = serde_json::to_vec(&input)
            .map_err(|_| StorageError::Serialization("serialize audit digest"))?;
        let mut digest = Sha256::new();
        digest.update(bytes);
        Ok(digest.finalize().into())
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AuditDigestInput<'a> {
    event_id: Uuid,
    audit_version: u16,
    workspace_id: Uuid,
    occurred_at: DateTime<Utc>,
    actor: &'a AuditActor,
    action: &'a str,
    reason_code: &'a str,
    request_id: &'a str,
    correlation_id: &'a Option<String>,
    trace_id: &'a Option<String>,
    object: &'a AuditObjectRef,
    before: &'a Option<Value>,
    after: &'a Option<Value>,
    lineage: &'a [AuditLineageEdge],
    source_event_id: &'a Option<Uuid>,
    payload: &'a Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditEventRecord {
    pub event_id: Uuid,
    pub audit_version: u16,
    pub workspace_id: Uuid,
    pub sequence: u64,
    pub occurred_at: DateTime<Utc>,
    pub actor: AuditActor,
    pub action: String,
    pub reason_code: String,
    pub request_id: String,
    pub correlation_id: Option<String>,
    pub trace_id: Option<String>,
    pub object: AuditObjectRef,
    pub before: Option<Value>,
    pub after: Option<Value>,
    pub lineage: Vec<AuditLineageEdge>,
    pub source_event_id: Option<Uuid>,
    pub payload: Value,
    pub idempotency_key: Option<String>,
    pub event_digest: [u8; 32],
    pub retention: AuditRetentionState,
    pub expired_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditCursor {
    pub workspace_id: Uuid,
    pub sequence: u64,
    pub filter_digest: [u8; 32],
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct AuditQuery {
    pub workspace_id: Uuid,
    pub actor_kind: Option<AuditActorKind>,
    pub actor_ref: Option<String>,
    pub object_kind: Option<String>,
    pub object_id: Option<Uuid>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    pub action: Option<String>,
    pub trace_id: Option<String>,
    pub correlation_id: Option<String>,
    pub include_expired: bool,
    pub limit: usize,
    pub cursor: Option<AuditCursor>,
}

impl AuditQuery {
    pub fn filter_digest(&self) -> Result<[u8; 32], StorageError> {
        let input = AuditQueryScope {
            workspace_id: self.workspace_id,
            actor_kind: self.actor_kind,
            actor_ref: &self.actor_ref,
            object_kind: &self.object_kind,
            object_id: self.object_id,
            from: self.from,
            to: self.to,
            action: &self.action,
            trace_id: &self.trace_id,
            correlation_id: &self.correlation_id,
            include_expired: self.include_expired,
        };
        let bytes = serde_json::to_vec(&input)
            .map_err(|_| StorageError::Serialization("serialize audit query scope"))?;
        let mut digest = Sha256::new();
        digest.update(bytes);
        Ok(digest.finalize().into())
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AuditQueryScope<'a> {
    workspace_id: Uuid,
    actor_kind: Option<AuditActorKind>,
    actor_ref: &'a Option<String>,
    object_kind: &'a Option<String>,
    object_id: Option<Uuid>,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
    action: &'a Option<String>,
    trace_id: &'a Option<String>,
    correlation_id: &'a Option<String>,
    include_expired: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AuditPage {
    pub events: Vec<AuditEventRecord>,
    pub next: Option<AuditCursor>,
}

#[derive(Clone)]
pub struct AuditStore {
    inner: Arc<StoreInner>,
}

impl fmt::Debug for AuditStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuditStore")
            .field("audit_version", &AUDIT_VERSION)
            .finish_non_exhaustive()
    }
}

impl AuditStore {
    pub(crate) fn from_inner(inner: Arc<StoreInner>) -> Self {
        Self { inner }
    }

    pub fn append(&self, draft: AuditEventDraft) -> Result<AuditEventRecord, StorageError> {
        draft.validate()?;
        let digest = draft.digest()?;
        let _activity = acquire_activity(&self.inner, ActivityKind::Publisher)?;
        let mut connection = open_connection(&self.inner)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StorageError::database("begin audit append"))?;
        let workspace_exists: Option<i64> = transaction
            .query_row(
                "SELECT 1 FROM cp_workspaces WHERE id = ?1",
                [draft.workspace_id.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(|_| StorageError::database("check audit workspace"))?;
        if workspace_exists.is_none() {
            return Err(StorageError::NotFound(draft.workspace_id));
        }

        let existing: Option<AuditEventRecord> = transaction
            .query_row(
                "SELECT event_id, audit_version, workspace_id, sequence,
                        occurred_at_utc, actor_kind, actor_ref, action, reason_code,
                        request_id, correlation_id, trace_id, object_kind, object_id,
                        before_json, after_json, lineage_json, source_event_id,
                        payload_json, idempotency_key, event_digest, retention_state,
                        expired_at_utc
                 FROM audit_events
                 WHERE workspace_id = ?1 AND (event_id = ?2 OR
                       (?3 IS NOT NULL AND idempotency_key = ?3))
                 LIMIT 1",
                params![
                    draft.workspace_id.to_string(),
                    draft.event_id.to_string(),
                    draft.idempotency_key.as_deref(),
                ],
                record_from_row,
            )
            .optional()
            .map_err(|_| StorageError::database("read existing audit event"))?;
        if let Some(existing) = existing {
            if existing.event_digest == digest {
                transaction
                    .commit()
                    .map_err(|_| StorageError::database("commit audit replay"))?;
                return Ok(existing);
            }
            return Err(StorageError::InvalidDraft(
                "audit identity or idempotency key was reused with a different event",
            ));
        }

        let max_sequence: i64 = transaction
            .query_row(
                "SELECT COALESCE(MAX(sequence), 0) FROM audit_events WHERE workspace_id = ?1",
                [draft.workspace_id.to_string()],
                |row| row.get(0),
            )
            .map_err(|_| StorageError::database("read audit sequence"))?;
        let sequence = u64::try_from(
            max_sequence
                .checked_add(1)
                .ok_or(StorageError::ArithmeticOverflow("audit sequence"))?,
        )
        .map_err(|_| StorageError::ArithmeticOverflow("audit sequence"))?;
        let before_json = serialize_optional(&draft.before, "serialize audit before")?;
        let after_json = serialize_optional(&draft.after, "serialize audit after")?;
        let lineage_json = serde_json::to_string(&draft.lineage)
            .map_err(|_| StorageError::Serialization("serialize audit lineage"))?;
        let payload_json = serde_json::to_string(&draft.payload)
            .map_err(|_| StorageError::Serialization("serialize audit payload"))?;
        transaction
            .execute(
                "INSERT INTO audit_events
                 (event_id, audit_version, workspace_id, sequence, occurred_at_utc,
                  actor_kind, actor_ref, action, reason_code, request_id,
                  correlation_id, trace_id, object_kind, object_id, before_json,
                  after_json, lineage_json, source_event_id, payload_json,
                  idempotency_key, event_digest, retention_state, expired_at_utc)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12,
                         ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, 'active', NULL)",
                params![
                    draft.event_id.to_string(),
                    i64::from(draft.audit_version),
                    draft.workspace_id.to_string(),
                    i64::try_from(sequence)
                        .map_err(|_| StorageError::ArithmeticOverflow("audit sequence"))?,
                    format_timestamp(&draft.occurred_at),
                    draft.actor.kind.as_str(),
                    draft.actor.actor_ref,
                    draft.action,
                    draft.reason_code,
                    draft.request_id,
                    draft.correlation_id,
                    draft.trace_id,
                    draft.object.kind,
                    draft.object.id.to_string(),
                    before_json,
                    after_json,
                    lineage_json,
                    draft.source_event_id.map(|value| value.to_string()),
                    payload_json,
                    draft.idempotency_key,
                    hex_encode(&digest),
                ],
            )
            .map_err(|_| StorageError::database("insert audit event"))?;
        let record = transaction
            .query_row(
                "SELECT event_id, audit_version, workspace_id, sequence,
                        occurred_at_utc, actor_kind, actor_ref, action, reason_code,
                        request_id, correlation_id, trace_id, object_kind, object_id,
                        before_json, after_json, lineage_json, source_event_id,
                        payload_json, idempotency_key, event_digest, retention_state,
                        expired_at_utc
                 FROM audit_events WHERE event_id = ?1",
                [draft.event_id.to_string()],
                record_from_row,
            )
            .map_err(|_| StorageError::database("read inserted audit event"))?;
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit audit append"))?;
        Ok(record)
    }

    pub fn query(&self, query: AuditQuery) -> Result<AuditPage, StorageError> {
        if query.workspace_id.is_nil() {
            return Err(StorageError::InvalidDraft(
                "audit workspace must not be nil",
            ));
        }
        if query.limit == 0 || query.limit > MAX_AUDIT_PAGE_SIZE {
            return Err(StorageError::InvalidDraft(
                "audit page size must be between 1 and 1000",
            ));
        }
        let filter_digest = query.filter_digest()?;
        if let Some(cursor) = &query.cursor {
            if cursor.workspace_id != query.workspace_id || cursor.filter_digest != filter_digest {
                return Err(StorageError::InvalidDraft(
                    "audit cursor is outside its workspace or filter scope",
                ));
            }
        }
        let _activity = acquire_activity(&self.inner, ActivityKind::Reader)?;
        let connection = open_connection(&self.inner)?;
        let mut clauses = vec!["workspace_id = ?".to_owned()];
        let mut values: Vec<Box<dyn ToSql>> = vec![Box::new(query.workspace_id.to_string())];
        if !query.include_expired {
            clauses.push("retention_state != 'expired'".to_owned());
        }
        if let Some(kind) = query.actor_kind {
            clauses.push("actor_kind = ?".to_owned());
            values.push(Box::new(kind.as_str().to_owned()));
        }
        if let Some(actor_ref) = query.actor_ref {
            clauses.push("actor_ref = ?".to_owned());
            values.push(Box::new(actor_ref));
        }
        if let Some(kind) = query.object_kind {
            clauses.push("(object_kind = ? OR lineage_json LIKE ?)".to_owned());
            values.push(Box::new(kind.clone()));
            values.push(Box::new(format!("%{kind}%")));
        }
        if let Some(object_id) = query.object_id {
            clauses.push("(object_id = ? OR lineage_json LIKE ?)".to_owned());
            values.push(Box::new(object_id.to_string()));
            values.push(Box::new(format!("%{object_id}%")));
        }
        if let Some(from) = query.from {
            clauses.push("occurred_at_utc >= ?".to_owned());
            values.push(Box::new(format_timestamp(&from)));
        }
        if let Some(to) = query.to {
            clauses.push("occurred_at_utc <= ?".to_owned());
            values.push(Box::new(format_timestamp(&to)));
        }
        if let Some(action) = query.action {
            clauses.push("action = ?".to_owned());
            values.push(Box::new(action));
        }
        if let Some(trace_id) = query.trace_id {
            clauses.push("trace_id = ?".to_owned());
            values.push(Box::new(trace_id));
        }
        if let Some(correlation_id) = query.correlation_id {
            clauses.push("correlation_id = ?".to_owned());
            values.push(Box::new(correlation_id));
        }
        if let Some(cursor) = query.cursor {
            clauses.push("sequence > ?".to_owned());
            values
                .push(Box::new(i64::try_from(cursor.sequence).map_err(|_| {
                    StorageError::ArithmeticOverflow("audit cursor")
                })?));
        }
        let sql = format!(
            "SELECT event_id, audit_version, workspace_id, sequence,
                    occurred_at_utc, actor_kind, actor_ref, action, reason_code,
                    request_id, correlation_id, trace_id, object_kind, object_id,
                    before_json, after_json, lineage_json, source_event_id,
                    payload_json, idempotency_key, event_digest, retention_state,
                    expired_at_utc
             FROM audit_events WHERE {} ORDER BY sequence ASC LIMIT ?",
            clauses.join(" AND ")
        );
        values.push(Box::new(i64::try_from(query.limit + 1).map_err(|_| {
            StorageError::ArithmeticOverflow("audit page limit")
        })?));
        let mut statement = connection
            .prepare(&sql)
            .map_err(|_| StorageError::database("prepare audit page"))?;
        let rows = statement
            .query_map(
                params_from_iter(values.iter().map(|value| value.as_ref())),
                record_from_row,
            )
            .map_err(|_| StorageError::database("read audit page"))?;
        let mut events = Vec::with_capacity(query.limit);
        for row in rows {
            events.push(row.map_err(|_| StorageError::database("decode audit page"))?);
        }
        let next = if events.len() > query.limit {
            events.pop();
            events.last().map(|event| AuditCursor {
                workspace_id: query.workspace_id,
                sequence: event.sequence,
                filter_digest,
            })
        } else {
            None
        };
        Ok(AuditPage { events, next })
    }

    /// Marks a record expired while retaining its immutable identity and
    /// digest. The retention worker in a later node may call this method; the
    /// AUD-A1 API only controls whether expired rows are visible.
    pub fn expire(
        &self,
        event_id: Uuid,
        expired_at: DateTime<Utc>,
    ) -> Result<AuditEventRecord, StorageError> {
        if event_id.is_nil() {
            return Err(StorageError::InvalidDraft("audit event must not be nil"));
        }
        let _activity = acquire_activity(&self.inner, ActivityKind::Publisher)?;
        let mut connection = open_connection(&self.inner)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StorageError::database("begin audit expiry"))?;
        let changed = transaction
            .execute(
                "UPDATE audit_events SET retention_state = 'expired', expired_at_utc = ?2
                 WHERE event_id = ?1 AND retention_state != 'expired'",
                params![event_id.to_string(), format_timestamp(&expired_at)],
            )
            .map_err(|_| StorageError::database("expire audit event"))?;
        if changed == 0 {
            let present: Option<i64> = transaction
                .query_row(
                    "SELECT 1 FROM audit_events WHERE event_id = ?1",
                    [event_id.to_string()],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|_| StorageError::database("check audit expiry"))?;
            if present.is_none() {
                return Err(StorageError::NotFound(event_id));
            }
        }
        let record = transaction
            .query_row(
                "SELECT event_id, audit_version, workspace_id, sequence,
                        occurred_at_utc, actor_kind, actor_ref, action, reason_code,
                        request_id, correlation_id, trace_id, object_kind, object_id,
                        before_json, after_json, lineage_json, source_event_id,
                        payload_json, idempotency_key, event_digest, retention_state,
                        expired_at_utc
                 FROM audit_events WHERE event_id = ?1",
                [event_id.to_string()],
                record_from_row,
            )
            .map_err(|_| StorageError::database("read expired audit event"))?;
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit audit expiry"))?;
        Ok(record)
    }
}

/// Appends a system audit receipt inside an already-held maintenance
/// transaction. Normal callers use `AuditStore::append`, which acquires a
/// publisher activity guard; retention must instead keep its tombstone and
/// deletion receipt in the same transaction while the maintenance gate is
/// held.
pub(crate) fn append_maintenance_audit_tx(
    transaction: &Transaction<'_>,
    draft: AuditEventDraft,
) -> Result<(), StorageError> {
    draft.validate()?;
    let digest = draft.digest()?;
    let workspace_exists: Option<i64> = transaction
        .query_row(
            "SELECT 1 FROM cp_workspaces WHERE id = ?1",
            [draft.workspace_id.to_string()],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| StorageError::database("check maintenance audit workspace"))?;
    if workspace_exists.is_none() {
        return Err(StorageError::NotFound(draft.workspace_id));
    }
    let existing: Option<String> = transaction
        .query_row(
            "SELECT event_digest FROM audit_events
             WHERE workspace_id = ?1 AND (event_id = ?2 OR
                   (?3 IS NOT NULL AND idempotency_key = ?3))
             LIMIT 1",
            params![
                draft.workspace_id.to_string(),
                draft.event_id.to_string(),
                draft.idempotency_key.as_deref(),
            ],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| StorageError::database("read maintenance audit receipt"))?;
    if let Some(existing) = existing {
        if existing == hex_encode(&digest) {
            return Ok(());
        }
        return Err(StorageError::InvalidDraft(
            "maintenance audit idempotency key was reused with a different receipt",
        ));
    }
    let max_sequence: i64 = transaction
        .query_row(
            "SELECT COALESCE(MAX(sequence), 0) FROM audit_events WHERE workspace_id = ?1",
            [draft.workspace_id.to_string()],
            |row| row.get(0),
        )
        .map_err(|_| StorageError::database("read maintenance audit sequence"))?;
    let sequence = u64::try_from(max_sequence.checked_add(1).ok_or(
        StorageError::ArithmeticOverflow("maintenance audit sequence"),
    )?)
    .map_err(|_| StorageError::ArithmeticOverflow("maintenance audit sequence"))?;
    let before_json = serialize_optional(&draft.before, "serialize maintenance audit before")?;
    let after_json = serialize_optional(&draft.after, "serialize maintenance audit after")?;
    let lineage_json = serde_json::to_string(&draft.lineage)
        .map_err(|_| StorageError::Serialization("serialize maintenance audit lineage"))?;
    let payload_json = serde_json::to_string(&draft.payload)
        .map_err(|_| StorageError::Serialization("serialize maintenance audit payload"))?;
    transaction
        .execute(
            "INSERT INTO audit_events
             (event_id, audit_version, workspace_id, sequence, occurred_at_utc,
              actor_kind, actor_ref, action, reason_code, request_id,
              correlation_id, trace_id, object_kind, object_id, before_json,
              after_json, lineage_json, source_event_id, payload_json,
              idempotency_key, event_digest, retention_state, expired_at_utc)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12,
                     ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, 'active', NULL)",
            params![
                draft.event_id.to_string(),
                i64::from(draft.audit_version),
                draft.workspace_id.to_string(),
                i64::try_from(sequence)
                    .map_err(|_| StorageError::ArithmeticOverflow("maintenance audit sequence"))?,
                format_timestamp(&draft.occurred_at),
                draft.actor.kind.as_str(),
                draft.actor.actor_ref,
                draft.action,
                draft.reason_code,
                draft.request_id,
                draft.correlation_id,
                draft.trace_id,
                draft.object.kind,
                draft.object.id.to_string(),
                before_json,
                after_json,
                lineage_json,
                draft.source_event_id.map(|value| value.to_string()),
                payload_json,
                draft.idempotency_key,
                hex_encode(&digest),
            ],
        )
        .map_err(|_| StorageError::database("insert maintenance audit receipt"))?;
    Ok(())
}

fn record_from_row(row: &Row<'_>) -> Result<AuditEventRecord, rusqlite::Error> {
    let before_json: Option<String> = row.get(14)?;
    let after_json: Option<String> = row.get(15)?;
    let lineage_json: String = row.get(16)?;
    let source_event_id: Option<String> = row.get(17)?;
    let payload_json: String = row.get(18)?;
    let digest: String = row.get(20)?;
    let digest = hex_decode(&digest).ok_or_else(|| rusqlite::Error::InvalidQuery)?;
    let event_digest: [u8; 32] = digest
        .try_into()
        .map_err(|_| rusqlite::Error::InvalidQuery)?;
    let object_id: String = row.get(13)?;
    let event_id: String = row.get(0)?;
    let workspace_id: String = row.get(2)?;
    let sequence: i64 = row.get(3)?;
    let occurred_at: String = row.get(4)?;
    let audit_version: i64 = row.get(1)?;
    let actor_kind: String = row.get(5)?;
    let actor_ref: String = row.get(6)?;
    let object_kind: String = row.get(12)?;
    let retention: String = row.get(21)?;
    let expired_at: Option<String> = row.get(22)?;
    Ok(AuditEventRecord {
        event_id: parse_uuid(&event_id)?,
        audit_version: u16::try_from(audit_version).map_err(|_| rusqlite::Error::InvalidQuery)?,
        workspace_id: parse_uuid(&workspace_id)?,
        sequence: u64::try_from(sequence).map_err(|_| rusqlite::Error::InvalidQuery)?,
        occurred_at: parse_timestamp(&occurred_at, "audit occurred timestamp")
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
        actor: AuditActor {
            kind: AuditActorKind::parse(&actor_kind).map_err(|_| rusqlite::Error::InvalidQuery)?,
            actor_ref,
        },
        action: row.get(7)?,
        reason_code: row.get(8)?,
        request_id: row.get(9)?,
        correlation_id: row.get(10)?,
        trace_id: row.get(11)?,
        object: AuditObjectRef {
            kind: object_kind,
            id: parse_uuid(&object_id)?,
        },
        before: parse_optional_json(before_json)?,
        after: parse_optional_json(after_json)?,
        lineage: serde_json::from_str(&lineage_json).map_err(|_| rusqlite::Error::InvalidQuery)?,
        source_event_id: source_event_id.as_deref().map(parse_uuid).transpose()?,
        payload: serde_json::from_str(&payload_json).map_err(|_| rusqlite::Error::InvalidQuery)?,
        idempotency_key: row.get(19)?,
        event_digest,
        retention: AuditRetentionState::parse(&retention)
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
        expired_at: expired_at
            .as_deref()
            .map(|value| {
                parse_timestamp(value, "audit expiry timestamp")
                    .map_err(|_| rusqlite::Error::InvalidQuery)
            })
            .transpose()?,
    })
}

fn parse_uuid(value: &str) -> Result<Uuid, rusqlite::Error> {
    Uuid::parse_str(value).map_err(|_| rusqlite::Error::InvalidQuery)
}

fn parse_optional_json(value: Option<String>) -> Result<Option<Value>, rusqlite::Error> {
    value
        .map(|value| serde_json::from_str(&value).map_err(|_| rusqlite::Error::InvalidQuery))
        .transpose()
}

fn serialize_optional(
    value: &Option<Value>,
    label: &'static str,
) -> Result<Option<String>, StorageError> {
    value
        .as_ref()
        .map(|value| serde_json::to_string(value).map_err(|_| StorageError::Serialization(label)))
        .transpose()
}

fn validate_text(value: &str, label: &'static str) -> Result<(), StorageError> {
    if value.is_empty()
        || value.len() > MAX_AUDIT_TEXT_BYTES
        || value.trim() != value
        || value.chars().any(char::is_control)
        || value.to_ascii_lowercase().contains("password=")
        || value.to_ascii_lowercase().contains("token=")
        || value.to_ascii_lowercase().contains("secret=")
    {
        return Err(StorageError::InvalidDraft(label));
    }
    Ok(())
}

fn validate_optional_text(value: Option<&str>, label: &'static str) -> Result<(), StorageError> {
    if let Some(value) = value {
        validate_text(value, label)?;
    }
    Ok(())
}

fn hex_encode(bytes: &[u8; 32]) -> String {
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push_str(&format!("{byte:02x}"));
    }
    output
}

fn hex_decode(value: &str) -> Option<Vec<u8>> {
    if value.len() != 64 {
        return None;
    }
    let mut bytes = Vec::with_capacity(32);
    for pair in value.as_bytes().chunks_exact(2) {
        let high = hex_nibble(pair[0])?;
        let low = hex_nibble(pair[1])?;
        bytes.push((high << 4) | low);
    }
    Some(bytes)
}

fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}
