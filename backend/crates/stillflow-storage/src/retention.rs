//! Bounded, storage-owned retention and garbage collection.
//!
//! Retention is deliberately a maintenance operation, not a background
//! worker. It records a tombstone before collecting any object and keeps the
//! AUD-A1 audit stream intact. The existing Export GC remains owned by the
//! Export adapter; this module only invokes its bounded inner helper while a
//! single maintenance gate is held.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde_json::json;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::audit::{
    append_maintenance_audit_tx, AuditActor, AuditActorKind, AuditEventDraft, AuditObjectRef,
};
use crate::{
    acquire_activity, acquire_maintenance, format_timestamp, open_connection, parse_timestamp,
    GarbageCollectionReport, StorageError, StoreInner, MAX_MAINTENANCE_CANDIDATES,
};

pub const RETENTION_POLICY_VERSION: u16 = 1;
pub const MAX_RETENTION_DAYS: u64 = 3650;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RetentionObjectKind {
    Dataset,
    Snapshot,
    Artifact,
    Event,
    Run,
}

impl RetentionObjectKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Dataset => "dataset",
            Self::Snapshot => "snapshot",
            Self::Artifact => "artifact",
            Self::Event => "event",
            Self::Run => "run",
        }
    }

    fn parse(value: &str) -> Result<Self, StorageError> {
        match value {
            "dataset" => Ok(Self::Dataset),
            "snapshot" => Ok(Self::Snapshot),
            "artifact" => Ok(Self::Artifact),
            "event" => Ok(Self::Event),
            "run" => Ok(Self::Run),
            _ => Err(StorageError::InvalidDraft("unknown retention object kind")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionPolicy {
    version: u16,
    dataset: Duration,
    snapshot: Duration,
    artifact: Duration,
    event: Duration,
    run: Duration,
    max_candidates: u32,
}

impl RetentionPolicy {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        dataset: Duration,
        snapshot: Duration,
        artifact: Duration,
        event: Duration,
        run: Duration,
        max_candidates: u32,
    ) -> Result<Self, StorageError> {
        for (value, label) in [
            (dataset, "Dataset retention"),
            (snapshot, "Snapshot retention"),
            (artifact, "Artifact retention"),
            (event, "Event retention"),
            (run, "Run retention"),
        ] {
            if value.as_secs() > MAX_RETENTION_DAYS * 86_400 {
                return Err(StorageError::InvalidConfiguration(label));
            }
        }
        if max_candidates == 0 || max_candidates > MAX_MAINTENANCE_CANDIDATES {
            return Err(StorageError::InvalidConfiguration(
                "retention candidate limit is outside the supported range",
            ));
        }
        Ok(Self {
            version: RETENTION_POLICY_VERSION,
            dataset,
            snapshot,
            artifact,
            event,
            run,
            max_candidates,
        })
    }

    pub fn version(&self) -> u16 {
        self.version
    }

    pub fn dataset(&self) -> Duration {
        self.dataset
    }

    pub fn snapshot(&self) -> Duration {
        self.snapshot
    }

    pub fn artifact(&self) -> Duration {
        self.artifact
    }

    pub fn event(&self) -> Duration {
        self.event
    }

    pub fn run(&self) -> Duration {
        self.run
    }

    pub fn max_candidates(&self) -> u32 {
        self.max_candidates
    }

    fn duration(self, kind: RetentionObjectKind) -> Duration {
        match kind {
            RetentionObjectKind::Dataset => self.dataset,
            RetentionObjectKind::Snapshot => self.snapshot,
            RetentionObjectKind::Artifact => self.artifact,
            RetentionObjectKind::Event => self.event,
            RetentionObjectKind::Run => self.run,
        }
    }
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            version: RETENTION_POLICY_VERSION,
            dataset: Duration::from_secs(30 * 86_400),
            snapshot: Duration::from_secs(30 * 86_400),
            artifact: Duration::from_secs(30 * 86_400),
            event: Duration::from_secs(90 * 86_400),
            run: Duration::from_secs(90 * 86_400),
            max_candidates: MAX_MAINTENANCE_CANDIDATES,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionCandidate {
    pub kind: RetentionObjectKind,
    pub object_id: Uuid,
    pub workspace_id: Option<Uuid>,
    pub tombstoned_at: DateTime<Utc>,
    pub eligible_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionReport {
    dry_run: bool,
    examined: u32,
    tombstoned: u32,
    deleted: u32,
    retained: u32,
    audited: u32,
    candidates: Vec<RetentionCandidate>,
}

impl RetentionReport {
    pub fn is_dry_run(&self) -> bool {
        self.dry_run
    }

    pub fn examined(&self) -> u32 {
        self.examined
    }

    pub fn tombstoned(&self) -> u32 {
        self.tombstoned
    }

    pub fn deleted(&self) -> u32 {
        self.deleted
    }

    pub fn retained(&self) -> u32 {
        self.retained
    }

    pub fn audited(&self) -> u32 {
        self.audited
    }

    pub fn candidates(&self) -> &[RetentionCandidate] {
        &self.candidates
    }

    fn new(dry_run: bool) -> Self {
        Self {
            dry_run,
            examined: 0,
            tombstoned: 0,
            deleted: 0,
            retained: 0,
            audited: 0,
            candidates: Vec::new(),
        }
    }

    fn add_legacy(&mut self, report: &GarbageCollectionReport) {
        self.examined = self.examined.saturating_add(report.examined());
        self.deleted = self.deleted.saturating_add(report.deleted());
        self.retained = self.retained.saturating_add(report.retained());
    }
}

impl fmt::Display for RetentionObjectKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

pub(crate) fn plan(
    inner: &Arc<StoreInner>,
    now: DateTime<Utc>,
    policy: RetentionPolicy,
) -> Result<RetentionReport, StorageError> {
    validate_policy(policy)?;
    let _activity = acquire_activity(inner, crate::ActivityKind::Reader)?;
    let connection = open_connection(inner)?;
    let mut report = RetentionReport::new(true);
    collect_candidates(&connection, now, policy, &mut report)?;
    Ok(report)
}

pub(crate) fn collect(
    inner: &Arc<StoreInner>,
    now: DateTime<Utc>,
    policy: RetentionPolicy,
    dry_run: bool,
) -> Result<RetentionReport, StorageError> {
    validate_policy(policy)?;
    if dry_run {
        return plan(inner, now, policy);
    }

    let _maintenance = acquire_maintenance(inner)?;
    let mut connection = open_connection(inner)?;
    let transaction = connection
        .transaction()
        .map_err(|_| StorageError::database("begin retention transaction"))?;
    let mut report = RetentionReport::new(false);
    collect_candidates(&transaction, now, policy, &mut report)?;

    let mut audited_workspaces = BTreeSet::new();
    for candidate in report.candidates.clone() {
        if tombstone_candidate(&transaction, now, policy, &candidate)? {
            report.tombstoned = report.tombstoned.saturating_add(1);
        }
        if let Some(workspace_id) = candidate.workspace_id {
            audited_workspaces.insert(workspace_id);
        }
    }

    collect_tombstones(inner, &transaction, now, policy, &mut report)?;

    for workspace_id in audited_workspaces {
        append_retention_audit(&transaction, workspace_id, now, &report)?;
        report.audited = report.audited.saturating_add(1);
    }
    transaction
        .commit()
        .map_err(|_| StorageError::database("commit retention transaction"))?;

    // The legacy Snapshot/Export path remains separate, but shares this
    // maintenance gate and receipt so one bounded invocation is sufficient.
    let snapshot_cutoff = cutoff(now, policy.snapshot(), "Snapshot retention cutoff")?;
    let legacy = crate::store::collect_snapshot_export_garbage_inner(
        inner,
        &format_timestamp(&snapshot_cutoff),
        policy.max_candidates(),
    )?;
    report.add_legacy(&legacy);
    Ok(report)
}

fn validate_policy(policy: RetentionPolicy) -> Result<(), StorageError> {
    if policy.version != RETENTION_POLICY_VERSION {
        return Err(StorageError::InvalidConfiguration(
            "unsupported retention policy version",
        ));
    }
    RetentionPolicy::try_new(
        policy.dataset,
        policy.snapshot,
        policy.artifact,
        policy.event,
        policy.run,
        policy.max_candidates,
    )?;
    Ok(())
}

fn cutoff(
    now: DateTime<Utc>,
    retention: Duration,
    label: &'static str,
) -> Result<DateTime<Utc>, StorageError> {
    let duration = chrono::Duration::from_std(retention)
        .map_err(|_| StorageError::ArithmeticOverflow(label))?;
    now.checked_sub_signed(duration)
        .ok_or(StorageError::ArithmeticOverflow(label))
}

fn eligible_at(
    tombstoned_at: DateTime<Utc>,
    retention: Duration,
) -> Result<DateTime<Utc>, StorageError> {
    let duration = chrono::Duration::from_std(retention)
        .map_err(|_| StorageError::ArithmeticOverflow("retention eligibility"))?;
    tombstoned_at
        .checked_add_signed(duration)
        .ok_or(StorageError::ArithmeticOverflow("retention eligibility"))
}

fn add_candidate(
    report: &mut RetentionReport,
    policy: RetentionPolicy,
    kind: RetentionObjectKind,
    object_id: &str,
    workspace_id: Option<String>,
    tombstoned_at: &str,
) -> Result<(), StorageError> {
    if report.candidates.len() >= policy.max_candidates() as usize {
        return Ok(());
    }
    let object_id = Uuid::parse_str(object_id)
        .map_err(|_| StorageError::Serialization("retention object identity"))?;
    let workspace_id = workspace_id
        .as_deref()
        .map(|value| {
            Uuid::parse_str(value)
                .map_err(|_| StorageError::Serialization("retention workspace identity"))
        })
        .transpose()?;
    let tombstoned_at = parse_timestamp(tombstoned_at, "retention tombstone timestamp")?;
    let eligible_at = eligible_at(tombstoned_at, policy.duration(kind))?;
    report.examined = report.examined.saturating_add(1);
    report.candidates.push(RetentionCandidate {
        kind,
        object_id,
        workspace_id,
        tombstoned_at,
        eligible_at,
    });
    Ok(())
}

fn collect_candidates(
    connection: &Connection,
    now: DateTime<Utc>,
    policy: RetentionPolicy,
    report: &mut RetentionReport,
) -> Result<(), StorageError> {
    let dataset_cutoff =
        format_timestamp(&cutoff(now, policy.dataset(), "Dataset retention cutoff")?);
    let snapshot_cutoff = format_timestamp(&cutoff(
        now,
        policy.snapshot(),
        "Snapshot retention cutoff",
    )?);
    let artifact_cutoff = format_timestamp(&cutoff(
        now,
        policy.artifact(),
        "Artifact retention cutoff",
    )?);
    let event_cutoff = format_timestamp(&cutoff(now, policy.event(), "Event retention cutoff")?);
    let run_cutoff = format_timestamp(&cutoff(now, policy.run(), "Run retention cutoff")?);
    let remaining = |report: &RetentionReport| {
        i64::from(
            policy
                .max_candidates()
                .saturating_sub(report.candidates.len() as u32),
        )
    };

    if remaining(report) > 0 {
        let mut statement = connection
            .prepare(
                "SELECT d.id, d.workspace_id, d.created_at_utc
                 FROM cp_datasets d
                 WHERE d.state = 'archived' AND d.created_at_utc <= ?1
                   AND NOT EXISTS (SELECT 1 FROM snapshots s WHERE s.dataset_id = d.id)
                   AND NOT EXISTS (SELECT 1 FROM qd1_profile_history p WHERE p.dataset_id = d.id)
                 ORDER BY d.created_at_utc, d.id LIMIT ?2",
            )
            .map_err(|_| StorageError::database("prepare Dataset retention candidates"))?;
        let rows = statement
            .query_map(params![dataset_cutoff, remaining(report)], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(|_| StorageError::database("query Dataset retention candidates"))?;
        for row in rows {
            let (id, workspace, created) =
                row.map_err(|_| StorageError::database("read Dataset retention candidate"))?;
            add_candidate(
                report,
                policy,
                RetentionObjectKind::Dataset,
                &id,
                Some(workspace),
                &created,
            )?;
        }
    }

    if remaining(report) > 0 {
        let mut statement = connection
            .prepare(
                "SELECT s.id, d.workspace_id, s.created_at_utc,
                        COALESCE(s.tombstoned_at_utc, s.created_at_utc)
                 FROM snapshots s LEFT JOIN cp_datasets d ON d.id = s.dataset_id
                 WHERE ((s.state = 2 AND s.tombstoned_at_utc <= ?1)
                    OR (s.state = 1 AND d.state = 'archived' AND s.created_at_utc <= ?1))
                   AND NOT EXISTS (
                       SELECT 1 FROM cp_artifact_refs a
                       WHERE a.external_ref_kind = 'snapshot'
                         AND a.external_ref_id = s.id
                         AND a.state IN ('staged', 'committed'))
                 ORDER BY COALESCE(s.tombstoned_at_utc, s.created_at_utc), s.id LIMIT ?2",
            )
            .map_err(|_| StorageError::database("prepare Snapshot retention candidates"))?;
        let rows = statement
            .query_map(params![snapshot_cutoff, remaining(report)], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })
            .map_err(|_| StorageError::database("query Snapshot retention candidates"))?;
        for row in rows {
            let (id, workspace, _created, tombstoned) =
                row.map_err(|_| StorageError::database("read Snapshot retention candidate"))?;
            add_candidate(
                report,
                policy,
                RetentionObjectKind::Snapshot,
                &id,
                workspace,
                &tombstoned,
            )?;
        }
    }

    if remaining(report) > 0 {
        let mut statement = connection
            .prepare(
                "SELECT a.id, a.workspace_id, a.created_at_utc,
                        COALESCE(a.tombstoned_at_utc, a.created_at_utc)
                 FROM cp_artifact_refs a
                 WHERE a.state IN ('committed', 'tombstoned', 'failed')
                   AND COALESCE(a.tombstoned_at_utc, a.created_at_utc) <= ?1
                   AND NOT EXISTS (
                       SELECT 1 FROM qd1_profile_history p
                       WHERE p.profile_artifact_id = a.id)
                   AND NOT EXISTS (
                       SELECT 1 FROM qd1_drift_comparisons c
                       WHERE c.report_artifact_id = a.id)
                 ORDER BY COALESCE(a.tombstoned_at_utc, a.created_at_utc), a.id LIMIT ?2",
            )
            .map_err(|_| StorageError::database("prepare Artifact retention candidates"))?;
        let rows = statement
            .query_map(params![artifact_cutoff, remaining(report)], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })
            .map_err(|_| StorageError::database("query Artifact retention candidates"))?;
        for row in rows {
            let (id, workspace, _created, tombstoned) =
                row.map_err(|_| StorageError::database("read Artifact retention candidate"))?;
            add_candidate(
                report,
                policy,
                RetentionObjectKind::Artifact,
                &id,
                Some(workspace),
                &tombstoned,
            )?;
        }
    }

    if remaining(report) > 0 {
        let mut statement = connection
            .prepare(
                "SELECT e.event_id, e.workspace_id, e.occurred_at_utc
                 FROM cp_events e JOIN cp_jobs j ON j.id = e.job_id
                 WHERE j.state IN ('succeeded', 'failed', 'cancelled')
                   AND e.occurred_at_utc <= ?1
                 ORDER BY e.occurred_at_utc, e.event_id LIMIT ?2",
            )
            .map_err(|_| StorageError::database("prepare Event retention candidates"))?;
        let rows = statement
            .query_map(params![event_cutoff, remaining(report)], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(|_| StorageError::database("query Event retention candidates"))?;
        for row in rows {
            let (id, workspace, occurred) =
                row.map_err(|_| StorageError::database("read Event retention candidate"))?;
            add_candidate(
                report,
                policy,
                RetentionObjectKind::Event,
                &id,
                Some(workspace),
                &occurred,
            )?;
        }
    }

    if remaining(report) > 0 {
        let mut statement = connection
            .prepare(
                "SELECT r.id, r.workspace_id, r.finished_at_utc
                 FROM cp_runs r
                 WHERE r.state IN ('succeeded', 'failed', 'cancelled')
                   AND r.finished_at_utc IS NOT NULL AND r.finished_at_utc <= ?1
                   AND NOT EXISTS (
                       SELECT 1 FROM qd1_profile_history p
                       WHERE p.producing_run_id = r.id)
                   AND NOT EXISTS (
                       SELECT 1 FROM qd1_drift_comparisons c
                       WHERE c.producing_run_id = r.id)
                 ORDER BY r.finished_at_utc, r.id LIMIT ?2",
            )
            .map_err(|_| StorageError::database("prepare Run retention candidates"))?;
        let rows = statement
            .query_map(params![run_cutoff, remaining(report)], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(|_| StorageError::database("query Run retention candidates"))?;
        for row in rows {
            let (id, workspace, finished) =
                row.map_err(|_| StorageError::database("read Run retention candidate"))?;
            add_candidate(
                report,
                policy,
                RetentionObjectKind::Run,
                &id,
                Some(workspace),
                &finished,
            )?;
        }
    }
    Ok(())
}

fn tombstone_candidate(
    transaction: &Transaction<'_>,
    now: DateTime<Utc>,
    policy: RetentionPolicy,
    candidate: &RetentionCandidate,
) -> Result<bool, StorageError> {
    let now_text = format_timestamp(&now);
    let eligible = format_timestamp(&eligible_at(now, policy.duration(candidate.kind))?);
    let inserted = transaction
        .execute(
            "INSERT OR IGNORE INTO retention_tombstones
             (object_kind, object_id, workspace_id, tombstoned_at_utc,
              eligible_at_utc, reason)
             VALUES (?1, ?2, ?3, ?4, ?5, 'policy')",
            params![
                candidate.kind.as_str(),
                candidate.object_id.to_string(),
                candidate.workspace_id.map(|value| value.to_string()),
                if candidate.tombstoned_at < now {
                    format_timestamp(&candidate.tombstoned_at)
                } else {
                    now_text
                },
                eligible,
            ],
        )
        .map_err(|_| StorageError::database("persist retention tombstone"))?;
    if candidate.kind == RetentionObjectKind::Snapshot {
        let active_reference: Option<i64> = transaction
            .query_row(
                "SELECT 1 FROM cp_artifact_refs
                 WHERE external_ref_kind = 'snapshot' AND external_ref_id = ?1
                   AND state IN ('staged', 'committed') LIMIT 1",
                [candidate.object_id.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(|_| StorageError::database("check Snapshot retention references"))?;
        if active_reference.is_none() {
            transaction
                .execute(
                    "UPDATE snapshots SET state = 2, tombstoned_at_utc = COALESCE(tombstoned_at_utc, ?2)
                     WHERE id = ?1 AND state = 1",
                    params![candidate.object_id.to_string(), format_timestamp(&now)],
                )
                .map_err(|_| StorageError::database("tombstone Snapshot for retention"))?;
        }
    } else if candidate.kind == RetentionObjectKind::Artifact {
        transaction
            .execute(
                "UPDATE cp_artifact_refs SET state = 'tombstoned', tombstoned_at_utc = COALESCE(tombstoned_at_utc, ?2)
                 WHERE id = ?1 AND state = 'committed'",
                params![candidate.object_id.to_string(), format_timestamp(&now)],
            )
            .map_err(|_| StorageError::database("tombstone Artifact for retention"))?;
        transaction
            .execute(
                "UPDATE cp_artifact_bodies SET state = 'tombstoned'
                 WHERE artifact_id = ?1 AND state = 'committed'",
                [candidate.object_id.to_string()],
            )
            .map_err(|_| StorageError::database("tombstone Artifact body for retention"))?;
    }
    Ok(inserted == 1)
}

fn collect_tombstones(
    inner: &Arc<StoreInner>,
    transaction: &Transaction<'_>,
    now: DateTime<Utc>,
    policy: RetentionPolicy,
    report: &mut RetentionReport,
) -> Result<(), StorageError> {
    let mut statement = transaction
        .prepare(
            "SELECT object_kind, object_id
             FROM retention_tombstones
             WHERE eligible_at_utc <= ?1
             ORDER BY CASE object_kind
                 WHEN 'run' THEN 1 WHEN 'event' THEN 2 WHEN 'artifact' THEN 3
                 WHEN 'snapshot' THEN 4 WHEN 'dataset' THEN 5 ELSE 6 END,
                 eligible_at_utc, object_id LIMIT ?2",
        )
        .map_err(|_| StorageError::database("prepare eligible retention tombstones"))?;
    let rows = statement
        .query_map(
            params![format_timestamp(&now), i64::from(policy.max_candidates())],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .map_err(|_| StorageError::database("query eligible retention tombstones"))?;
    let mut tombstones = Vec::new();
    for row in rows {
        tombstones
            .push(row.map_err(|_| StorageError::database("read eligible retention tombstone"))?);
    }
    drop(statement);

    for (kind, object_id) in tombstones {
        let kind = RetentionObjectKind::parse(&kind)?;
        let object_id = Uuid::parse_str(&object_id)
            .map_err(|_| StorageError::Serialization("retention tombstone identity"))?;
        if delete_candidate(inner, transaction, kind, object_id)? {
            report.deleted = report.deleted.saturating_add(1);
            transaction
                .execute(
                    "DELETE FROM retention_tombstones WHERE object_kind = ?1 AND object_id = ?2",
                    params![kind.as_str(), object_id.to_string()],
                )
                .map_err(|_| StorageError::database("delete retention tombstone"))?;
        } else {
            report.retained = report.retained.saturating_add(1);
        }
    }
    Ok(())
}

fn delete_candidate(
    inner: &Arc<StoreInner>,
    transaction: &Transaction<'_>,
    kind: RetentionObjectKind,
    object_id: Uuid,
) -> Result<bool, StorageError> {
    let id = object_id.to_string();
    match kind {
        RetentionObjectKind::Dataset => {
            let blocked: i64 = transaction
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM snapshots WHERE dataset_id = ?1)
                            OR EXISTS(SELECT 1 FROM qd1_profile_history WHERE dataset_id = ?1)
                            OR EXISTS(SELECT 1 FROM qd1_drift_comparisons WHERE dataset_id = ?1)",
                    [&id],
                    |row| row.get(0),
                )
                .map_err(|_| StorageError::database("check Dataset retention references"))?;
            if blocked != 0 {
                return Ok(false);
            }
            let deleted = transaction
                .execute(
                    "DELETE FROM cp_datasets WHERE id = ?1 AND state = 'archived'",
                    [&id],
                )
                .map_err(|_| StorageError::database("delete Dataset retention candidate"))?;
            Ok(deleted == 1 || !exists(transaction, "cp_datasets", &id)?)
        }
        RetentionObjectKind::Snapshot => {
            if !exists(transaction, "snapshots", &id)? {
                return Ok(true);
            }
            let blocked: i64 = transaction
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM cp_artifact_refs
                                   WHERE external_ref_kind = 'snapshot' AND external_ref_id = ?1
                                     AND state IN ('staged', 'committed'))",
                    [&id],
                    |row| row.get(0),
                )
                .map_err(|_| StorageError::database("check Snapshot collection references"))?;
            if blocked != 0 {
                return Ok(false);
            }
            if !crate::store::delete_tombstoned_snapshot_files(inner, object_id)? {
                return Ok(false);
            }
            let deleted = transaction
                .execute("DELETE FROM snapshots WHERE id = ?1 AND state = 2", [&id])
                .map_err(|_| StorageError::database("delete Snapshot retention candidate"))?;
            Ok(deleted == 1 || !exists(transaction, "snapshots", &id)?)
        }
        RetentionObjectKind::Artifact => {
            if !exists(transaction, "cp_artifact_refs", &id)? {
                return Ok(true);
            }
            let blocked: i64 = transaction
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM qd1_profile_history WHERE profile_artifact_id = ?1)
                            OR EXISTS(SELECT 1 FROM qd1_drift_comparisons WHERE report_artifact_id = ?1)
                            OR EXISTS(SELECT 1 FROM cp_runs r JOIN cp_artifact_refs a ON a.run_id = r.id
                                       WHERE a.id = ?1)",
                    [&id],
                    |row| row.get(0),
                )
                .map_err(|_| StorageError::database("check Artifact retention references"))?;
            if blocked != 0 {
                return Ok(false);
            }
            transaction
                .execute(
                    "DELETE FROM cp_artifact_bodies WHERE artifact_id = ?1",
                    [&id],
                )
                .map_err(|_| StorageError::database("delete Artifact body for retention"))?;
            transaction
                .execute("DELETE FROM cp_artifact_refs WHERE id = ?1", [&id])
                .map_err(|_| StorageError::database("delete Artifact retention candidate"))?;
            Ok(true)
        }
        RetentionObjectKind::Event => {
            if !exists(transaction, "cp_events", &id)? {
                return Ok(true);
            }
            let deleted = transaction
                .execute(
                    "DELETE FROM cp_events WHERE event_id = ?1 AND EXISTS(
                         SELECT 1 FROM cp_jobs j WHERE j.id = cp_events.job_id
                           AND j.state IN ('succeeded', 'failed', 'cancelled'))",
                    [&id],
                )
                .map_err(|_| StorageError::database("delete Event retention candidate"))?;
            Ok(deleted == 1)
        }
        RetentionObjectKind::Run => {
            if !exists(transaction, "cp_runs", &id)? {
                return Ok(true);
            }
            let blocked: i64 = transaction
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM qd1_profile_history WHERE producing_run_id = ?1)
                            OR EXISTS(SELECT 1 FROM qd1_drift_comparisons WHERE producing_run_id = ?1)",
                    [&id],
                    |row| row.get(0),
                )
                .map_err(|_| StorageError::database("check Run retention references"))?;
            if blocked != 0 {
                return Ok(false);
            }
            let job_id: String = transaction
                .query_row("SELECT job_id FROM cp_runs WHERE id = ?1", [&id], |row| {
                    row.get(0)
                })
                .map_err(|_| StorageError::database("read Run retention Job"))?;
            transaction
                .execute("DELETE FROM cp_artifact_bodies WHERE artifact_id IN (SELECT id FROM cp_artifact_refs WHERE run_id = ?1)", [&id])
                .map_err(|_| StorageError::database("delete Run Artifact bodies"))?;
            transaction
                .execute("DELETE FROM cp_artifact_refs WHERE run_id = ?1", [&id])
                .map_err(|_| StorageError::database("delete Run Artifact refs"))?;
            transaction
                .execute("DELETE FROM cp_events WHERE job_id = ?1", [&job_id])
                .map_err(|_| StorageError::database("delete Run lifecycle events"))?;
            transaction
                .execute(
                    "DELETE FROM cp_idempotency_keys WHERE job_id = ?1",
                    [&job_id],
                )
                .map_err(|_| StorageError::database("delete Run idempotency record"))?;
            transaction
                .execute("DELETE FROM cp_runs WHERE id = ?1 AND state IN ('succeeded', 'failed', 'cancelled')", [&id])
                .map_err(|_| StorageError::database("delete Run retention candidate"))?;
            transaction
                .execute("DELETE FROM cp_jobs WHERE id = ?1 AND state IN ('succeeded', 'failed', 'cancelled')", [&job_id])
                .map_err(|_| StorageError::database("delete Job for Run retention"))?;
            Ok(!exists(transaction, "cp_runs", &id)?)
        }
    }
}

fn exists(
    transaction: &Transaction<'_>,
    table: &str,
    object_id: &str,
) -> Result<bool, StorageError> {
    // Table names are fixed at each call site; no caller-controlled SQL is
    // accepted here.
    let query = format!("SELECT EXISTS(SELECT 1 FROM {table} WHERE id = ?1)");
    transaction
        .query_row(&query, [object_id], |row| row.get::<_, i64>(0))
        .map(|value| value != 0)
        .map_err(|_| StorageError::database("check retention object existence"))
}

fn append_retention_audit(
    transaction: &Transaction<'_>,
    workspace_id: Uuid,
    now: DateTime<Utc>,
    report: &RetentionReport,
) -> Result<(), StorageError> {
    let mut digest_input = Sha256::new();
    digest_input.update(workspace_id.as_bytes());
    digest_input.update(format_timestamp(&now).as_bytes());
    let digest = digest_input.finalize();
    let event_id = Uuid::from_bytes(digest[..16].try_into().expect("16-byte digest prefix"));
    let idempotency = format!("ops-o2:{}:{}", workspace_id, format_timestamp(&now));
    let payload = json!({
        "policyVersion": RETENTION_POLICY_VERSION,
        "dryRun": false,
        "examined": report.examined,
        "tombstoned": report.tombstoned,
        "deleted": report.deleted,
        "retained": report.retained,
    });
    let draft = AuditEventDraft {
        event_id,
        audit_version: crate::AUDIT_VERSION,
        workspace_id,
        occurred_at: now,
        actor: AuditActor {
            kind: AuditActorKind::System,
            actor_ref: "system:retention".to_owned(),
        },
        action: "retention.collect".to_owned(),
        reason_code: "ops-o2-policy".to_owned(),
        request_id: idempotency.clone(),
        correlation_id: Some(idempotency.clone()),
        trace_id: None,
        object: AuditObjectRef {
            kind: "workspace".to_owned(),
            id: workspace_id,
        },
        before: None,
        after: Some(payload.clone()),
        lineage: Vec::new(),
        source_event_id: None,
        payload,
        idempotency_key: Some(idempotency),
    };
    append_maintenance_audit_tx(transaction, draft)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::{DateTime, Utc};
    use rusqlite::params;
    use tempfile::TempDir;

    use super::*;
    use crate::{ControlPlaneStore, SnapshotStore, StorageLimits};

    fn at(value: &str) -> DateTime<Utc> {
        value.parse().expect("valid test timestamp")
    }

    fn zero_policy() -> RetentionPolicy {
        RetentionPolicy::try_new(
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            16,
        )
        .expect("valid zero-retention policy")
    }

    #[test]
    fn policy_rejects_unbounded_values() {
        assert!(RetentionPolicy::try_new(
            Duration::from_secs(MAX_RETENTION_DAYS * 86_400 + 1),
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
            1,
        )
        .is_err());
        assert!(RetentionPolicy::try_new(
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            0,
        )
        .is_err());
    }

    #[test]
    fn archived_dataset_collection_is_reference_safe_audited_and_idempotent() {
        let temp = TempDir::new().expect("temp directory");
        let store = SnapshotStore::open(temp.path(), StorageLimits::default()).expect("store");
        let workspace_id = Uuid::from_u128(1);
        let session_id = Uuid::from_u128(2);
        let connection_id = Uuid::from_u128(3);
        let asset_id = Uuid::from_u128(4);
        let archived_dataset_id = Uuid::from_u128(5);
        let active_dataset_id = Uuid::from_u128(6);
        let created_at = at("2025-01-01T00:00:00Z");
        let now = at("2025-02-01T00:00:00Z");
        let created = format_timestamp(&created_at);
        let connection = open_connection(&store.inner).expect("open metadata database");

        connection
            .execute(
                "INSERT INTO cp_workspaces
                 (id, state, created_at_utc, archived_at_utc)
                 VALUES (?1, 'active', ?2, NULL)",
                params![workspace_id.to_string(), created],
            )
            .expect("workspace row");
        connection
            .execute(
                "INSERT INTO cp_sessions
                 (id, workspace_id, state, created_at_utc, updated_at_utc)
                 VALUES (?1, ?2, 'closed', ?3, ?3)",
                params![session_id.to_string(), workspace_id.to_string(), created],
            )
            .expect("session row");
        connection
            .execute(
                "INSERT INTO cp_connections
                 (id, workspace_id, connector_kind, name, config_json,
                  credential_ref, state, created_at_utc, updated_at_utc)
                 VALUES (?1, ?2, 'file', 'test', '{}', 'cred://test',
                         'active', ?3, ?3)",
                params![connection_id.to_string(), workspace_id.to_string(), created],
            )
            .expect("connection row");
        connection
            .execute(
                "INSERT INTO cp_assets
                 (id, workspace_id, connection_id, asset_kind, name,
                  locator_json, state, discovered_at_utc)
                 VALUES (?1, ?2, ?3, 'file', 'test', '{}', 'active', ?4)",
                params![
                    asset_id.to_string(),
                    workspace_id.to_string(),
                    connection_id.to_string(),
                    created,
                ],
            )
            .expect("asset row");
        for (dataset_id, state) in [
            (archived_dataset_id, "archived"),
            (active_dataset_id, "active"),
        ] {
            connection
                .execute(
                    "INSERT INTO cp_datasets
                     (id, workspace_id, session_id, source_asset_id, name,
                      state, created_at_utc)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        dataset_id.to_string(),
                        workspace_id.to_string(),
                        session_id.to_string(),
                        asset_id.to_string(),
                        state,
                        state,
                        created,
                    ],
                )
                .expect("dataset row");
        }
        drop(connection);

        let control_plane = ControlPlaneStore::from_snapshot_store(&store);
        let policy = zero_policy();
        let planned = control_plane
            .retention_plan(now, policy)
            .expect("retention plan");
        assert!(planned.is_dry_run());
        assert_eq!(planned.examined(), 1);
        assert_eq!(planned.candidates()[0].kind, RetentionObjectKind::Dataset);

        let collected = control_plane
            .collect_retention(now, policy, false)
            .expect("retention collection");
        assert!(!collected.is_dry_run());
        assert_eq!(collected.examined(), 1);
        assert_eq!(collected.tombstoned(), 1);
        assert_eq!(collected.deleted(), 1);
        assert_eq!(collected.retained(), 0);
        assert_eq!(collected.audited(), 1);

        let connection = open_connection(&store.inner).expect("reopen metadata database");
        let archived_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM cp_datasets WHERE id = ?1",
                [archived_dataset_id.to_string()],
                |row| row.get(0),
            )
            .expect("archived dataset count");
        let active_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM cp_datasets WHERE id = ?1 AND state = 'active'",
                [active_dataset_id.to_string()],
                |row| row.get(0),
            )
            .expect("active dataset count");
        let audit_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM audit_events WHERE action = 'retention.collect'",
                [],
                |row| row.get(0),
            )
            .expect("retention audit count");
        assert_eq!(archived_count, 0);
        assert_eq!(active_count, 1);
        assert_eq!(audit_count, 1);
        drop(connection);

        let repeated = control_plane
            .collect_retention(now, policy, false)
            .expect("repeat retention collection");
        assert_eq!(repeated.examined(), 0);
        assert_eq!(repeated.tombstoned(), 0);
        assert_eq!(repeated.deleted(), 0);
        assert_eq!(repeated.audited(), 0);
    }
}
