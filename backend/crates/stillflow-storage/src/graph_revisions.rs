//! The immutable GraphRevision store (NX-V1, #343; frozen by the NX-V0
//! contract §3/§4/§7).
//!
//! Revisions are append-only facts: the store implements first-save, CAS
//! save, idempotent save, current/point-in-time fetch, history, the
//! append-only PlanVersion link on the newest row, and the migrated-revision
//! append. There is no update of content and no delete path.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use rusqlite::params;
use serde_json::Value;
use uuid::Uuid;

use crate::{map_constraint, open_connection, StorageError, StoreInner};

/// One stored revision. `graph_json` is the byte-preserved canonical graph
/// document; `graph_digest` is its SHA-256 (64 hex).
#[derive(Debug, Clone, PartialEq)]
pub struct GraphRevisionRecord {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub graph_id: Uuid,
    pub revision_number: u64,
    pub parent_revision_id: Option<Uuid>,
    pub format_version: u16,
    pub graph_json: String,
    pub graph_digest: String,
    pub source_binding: Option<Value>,
    pub package_digests: Option<Value>,
    pub compiler_version: Option<String>,
    pub plan_version_id: Option<Uuid>,
    pub migration: Option<Value>,
    pub created_at: DateTime<Utc>,
    pub created_by: String,
}

/// The optimistic-concurrency expectation (NX-V0 §4.1): the caller names the
/// revision it based its edit on, optionally with the digest it saw.
#[derive(Debug, Clone, PartialEq)]
pub struct ExpectedRevision {
    pub number: u64,
    pub digest: Option<String>,
}

/// The content of a new revision (everything except identity, numbering,
/// and the parent link).
#[derive(Debug, Clone, PartialEq)]
pub struct RevisionDraft {
    pub format_version: u16,
    pub graph_json: String,
    pub graph_digest: String,
    pub source_binding: Option<Value>,
    pub package_digests: Option<Value>,
    pub compiler_version: Option<String>,
    pub created_by: String,
}

/// The outcome of a save: a new revision, or the unchanged current revision
/// (idempotent same-digest save).
#[derive(Debug, Clone, PartialEq)]
pub enum GraphRevisionSave {
    Saved(GraphRevisionRecord),
    Current(GraphRevisionRecord),
}

#[derive(Clone)]
pub struct GraphRevisionStore {
    inner: Arc<StoreInner>,
}

impl GraphRevisionStore {
    pub(crate) fn from_inner(inner: Arc<StoreInner>) -> Self {
        Self { inner }
    }

    /// Saves a revision per the NX-V0 §4.1 law: first save creates revision
    /// 1; a CAS save with a matching expectation appends `current + 1`; a
    /// same-digest save is a no-op returning the current revision; a stale
    /// expectation fails with `AlreadyExists` naming the current revision.
    pub fn save(
        &self,
        workspace_id: Uuid,
        graph_id: Uuid,
        expected: Option<ExpectedRevision>,
        draft: RevisionDraft,
    ) -> Result<GraphRevisionSave, StorageError> {
        let _activity =
            crate::store::acquire_activity(&self.inner, crate::store::ActivityKind::Publisher)?;
        let mut connection = open_connection(&self.inner)?;
        let transaction = connection
            .transaction()
            .map_err(|_| StorageError::database("begin graph revision save"))?;

        let current = current_revision(&transaction, workspace_id, graph_id)?;
        if let Some(current) = &current {
            if current.graph_digest == draft.graph_digest
                && current.format_version == draft.format_version
            {
                // Idempotent save: unchanged content never grows history.
                transaction
                    .commit()
                    .map_err(|_| StorageError::database("commit graph revision save"))?;
                return Ok(GraphRevisionSave::Current(current.clone()));
            }
        }
        if let Some(expected) = &expected {
            match &current {
                Some(current) if current.revision_number == expected.number => {
                    if let Some(expected_digest) = &expected.digest {
                        if &current.graph_digest != expected_digest {
                            return Err(conflict(current));
                        }
                    }
                }
                Some(current) => return Err(conflict(current)),
                None => return Err(StorageError::AlreadyExists(graph_id)),
            }
        }

        let next_number = current
            .as_ref()
            .map(|revision| revision.revision_number + 1)
            .unwrap_or(1);
        let parent = current.as_ref().map(|revision| revision.id);
        let revision_id = Uuid::new_v4();
        let now = Utc::now();
        transaction
            .execute(
                "INSERT INTO cp_graph_revisions
                 (id, workspace_id, graph_id, revision_number, parent_revision_id,
                  format_version, graph_json, graph_digest, source_binding_json,
                  package_digests_json, compiler_version, plan_version_id,
                  migration_json, created_at_utc, created_by)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, NULL, NULL, ?12, ?13)",
                params![
                    revision_id.to_string(),
                    workspace_id.to_string(),
                    graph_id.to_string(),
                    i64::try_from(next_number)
                        .map_err(|_| StorageError::ArithmeticOverflow("revision number"))?,
                    parent.map(|id| id.to_string()),
                    i64::from(draft.format_version),
                    draft.graph_json,
                    draft.graph_digest,
                    draft
                        .source_binding
                        .as_ref()
                        .map(serde_json::to_string)
                        .transpose()
                        .map_err(|_| StorageError::database("encode source binding"))?,
                    draft
                        .package_digests
                        .as_ref()
                        .map(serde_json::to_string)
                        .transpose()
                        .map_err(|_| StorageError::database("encode package digests"))?,
                    draft.compiler_version,
                    now.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
                    draft.created_by,
                ],
            )
            .map_err(|error| {
                eprintln!("DEBUG insert: {error}");
                map_constraint(error, revision_id)
            })?;
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit graph revision save"))?;
        let record = self
            .get(workspace_id, graph_id, Some(next_number))?
            .ok_or_else(|| StorageError::database("read back graph revision"))?;
        Ok(GraphRevisionSave::Saved(record))
    }

    /// Fetches one revision: `None` selects the current one.
    pub fn get(
        &self,
        workspace_id: Uuid,
        graph_id: Uuid,
        revision_number: Option<u64>,
    ) -> Result<Option<GraphRevisionRecord>, StorageError> {
        let _activity =
            crate::store::acquire_activity(&self.inner, crate::store::ActivityKind::Reader)?;
        let connection = open_connection(&self.inner)?;
        current_or_numbered(&connection, workspace_id, graph_id, revision_number)
    }

    /// The revision history, newest first (NX-V0 §7: never deleted).
    pub fn history(
        &self,
        workspace_id: Uuid,
        graph_id: Uuid,
        limit: usize,
    ) -> Result<Vec<GraphRevisionRecord>, StorageError> {
        let _activity =
            crate::store::acquire_activity(&self.inner, crate::store::ActivityKind::Reader)?;
        let connection = open_connection(&self.inner)?;
        let mut statement = connection
            .prepare(
                "SELECT id, revision_number, parent_revision_id, format_version, graph_json,
                        graph_digest, source_binding_json, package_digests_json,
                        compiler_version, plan_version_id, migration_json, created_at_utc,
                        created_by
                 FROM cp_graph_revisions
                 WHERE workspace_id = ?1 AND graph_id = ?2
                 ORDER BY revision_number DESC LIMIT ?3",
            )
            .map_err(|_| StorageError::database("prepare graph revision history"))?;
        let rows = statement
            .query_map(
                params![
                    workspace_id.to_string(),
                    graph_id.to_string(),
                    i64::try_from(limit)
                        .map_err(|_| StorageError::ArithmeticOverflow("revision history limit"))?
                ],
                |row| {
                    decode_row(row, workspace_id, graph_id)
                        .map_err(|_| rusqlite::Error::QueryReturnedNoRows)
                },
            )
            .map_err(|_| StorageError::database("read graph revision history"))?;
        let mut revisions = Vec::new();
        for row in rows {
            revisions.push(row.map_err(|_| StorageError::database("decode graph revision"))?);
        }
        Ok(revisions)
    }

    /// Links the newest revision to the PlanVersion produced from it
    /// (NX-V0 §4.3). This is the only narrow write on an existing row: the
    /// graph content, digest, and history stay untouched.
    pub fn link_plan_version(
        &self,
        workspace_id: Uuid,
        graph_id: Uuid,
        plan_version_id: Uuid,
    ) -> Result<GraphRevisionRecord, StorageError> {
        let _activity =
            crate::store::acquire_activity(&self.inner, crate::store::ActivityKind::Publisher)?;
        let mut connection = open_connection(&self.inner)?;
        let transaction = connection
            .transaction()
            .map_err(|_| StorageError::database("begin graph revision link"))?;
        let current = current_revision(&transaction, workspace_id, graph_id)?
            .ok_or(StorageError::IdentityNotFound)?;
        transaction
            .execute(
                "UPDATE cp_graph_revisions SET plan_version_id = ?4
                 WHERE workspace_id = ?1 AND graph_id = ?2 AND revision_number = ?3",
                params![
                    workspace_id.to_string(),
                    graph_id.to_string(),
                    i64::try_from(current.revision_number)
                        .map_err(|_| StorageError::ArithmeticOverflow("revision number"))?,
                    plan_version_id.to_string(),
                ],
            )
            .map_err(|error| map_constraint(error, graph_id))?;
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit graph revision link"))?;
        self.get(workspace_id, graph_id, Some(current.revision_number))?
            .ok_or_else(|| StorageError::database("read back linked revision"))
    }

    /// Appends a migrated revision (NX-V0 §5.3): one INSERT in one
    /// transaction — the parent is intact and the apply is retryable. The
    /// API layer owns idempotency (an already-at-target revision is
    /// returned unchanged) and the deterministic transform.
    pub fn append_migrated(
        &self,
        parent: &GraphRevisionRecord,
        to_format_version: u16,
        migrated_graph_json: String,
        migrated_digest: String,
        migration: Value,
    ) -> Result<GraphRevisionRecord, StorageError> {
        let draft = RevisionDraft {
            format_version: to_format_version,
            graph_json: migrated_graph_json,
            graph_digest: migrated_digest,
            source_binding: parent.source_binding.clone(),
            package_digests: parent.package_digests.clone(),
            compiler_version: parent.compiler_version.clone(),
            created_by: parent.created_by.clone(),
        };
        let _activity =
            crate::store::acquire_activity(&self.inner, crate::store::ActivityKind::Publisher)?;
        let mut connection = open_connection(&self.inner)?;
        let transaction = connection
            .transaction()
            .map_err(|_| StorageError::database("begin graph revision migration"))?;
        let current = current_revision(&transaction, parent.workspace_id, parent.graph_id)?;
        // The parent must still be the current revision: migration applies
        // to the tip, never to a historical row.
        if current.as_ref().map(|revision| revision.id) != Some(parent.id) {
            return Err(StorageError::AlreadyExists(parent.graph_id));
        }
        let next_number = current
            .as_ref()
            .map(|revision| revision.revision_number + 1)
            .unwrap_or(1);
        let revision_id = Uuid::new_v4();
        let now = Utc::now();
        transaction
            .execute(
                "INSERT INTO cp_graph_revisions
                 (id, workspace_id, graph_id, revision_number, parent_revision_id,
                  format_version, graph_json, graph_digest, source_binding_json,
                  package_digests_json, compiler_version, plan_version_id,
                  migration_json, created_at_utc, created_by)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, NULL, ?12, ?13, ?14)",
                params![
                    revision_id.to_string(),
                    parent.workspace_id.to_string(),
                    parent.graph_id.to_string(),
                    i64::try_from(next_number)
                        .map_err(|_| StorageError::ArithmeticOverflow("revision number"))?,
                    Some(parent.id.to_string()),
                    i64::from(to_format_version),
                    draft.graph_json,
                    draft.graph_digest,
                    draft
                        .source_binding
                        .as_ref()
                        .map(serde_json::to_string)
                        .transpose()
                        .map_err(|_| StorageError::database("encode source binding"))?,
                    draft
                        .package_digests
                        .as_ref()
                        .map(serde_json::to_string)
                        .transpose()
                        .map_err(|_| StorageError::database("encode package digests"))?,
                    draft.compiler_version,
                    serde_json::to_string(&migration)
                        .map_err(|_| StorageError::database("encode migration"))?,
                    now.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
                    draft.created_by,
                ],
            )
            .map_err(|error| map_constraint(error, revision_id))?;
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit graph revision migration"))?;
        self.get(parent.workspace_id, parent.graph_id, Some(next_number))?
            .ok_or_else(|| StorageError::database("read back migrated revision"))
    }
}

fn conflict(current: &GraphRevisionRecord) -> StorageError {
    // The API layer recognizes this variant for graph-revision saves and
    // enriches the conflict response with the current revision number and
    // digest (NX-V0 §4.1).
    StorageError::AlreadyExists(current.graph_id)
}

fn current_revision(
    connection: &rusqlite::Connection,
    workspace_id: Uuid,
    graph_id: Uuid,
) -> Result<Option<GraphRevisionRecord>, StorageError> {
    current_or_numbered(connection, workspace_id, graph_id, None)
}

fn current_or_numbered(
    connection: &rusqlite::Connection,
    workspace_id: Uuid,
    graph_id: Uuid,
    revision_number: Option<u64>,
) -> Result<Option<GraphRevisionRecord>, StorageError> {
    fetch_full(connection, workspace_id, graph_id, revision_number)
}

fn fetch_full(
    connection: &rusqlite::Connection,
    workspace_id: Uuid,
    graph_id: Uuid,
    revision_number: Option<u64>,
) -> Result<Option<GraphRevisionRecord>, StorageError> {
    let mut statement = connection
        .prepare(
            "SELECT id, revision_number, parent_revision_id, format_version, graph_json,
                    graph_digest, source_binding_json, package_digests_json, compiler_version,
                    plan_version_id, migration_json, created_at_utc, created_by
             FROM cp_graph_revisions
             WHERE workspace_id = ?1 AND graph_id = ?2
               AND (?3 IS NULL OR revision_number = CAST(?3 AS INTEGER))
             ORDER BY revision_number DESC
             LIMIT 1",
        )
        .map_err(|_| StorageError::database("prepare graph revision read"))?;
    let mut rows = statement
        .query(params![
            workspace_id.to_string(),
            graph_id.to_string(),
            revision_number
                .map(i64::try_from)
                .transpose()
                .map_err(|_| { StorageError::ArithmeticOverflow("revision number") })?
        ])
        .map_err(|_| StorageError::database("read graph revision"))?;
    match rows
        .next()
        .map_err(|_| StorageError::database("read graph revision"))?
    {
        Some(row) => {
            let record = decode_row(row, workspace_id, graph_id)
                .map_err(|_| StorageError::database("decode graph revision"))?;
            Ok(Some(record))
        }
        None => Ok(None),
    }
}

fn decode_row(
    row: &rusqlite::Row<'_>,
    workspace_id: Uuid,
    graph_id: Uuid,
) -> Result<GraphRevisionRecord, StorageError> {
    let id: String = rusqlite::Row::get(row, 0)
        .map_err(|_| StorageError::database("decode graph revision row"))?;
    let revision_number: i64 = rusqlite::Row::get(row, 1)
        .map_err(|_| StorageError::database("decode graph revision row"))?;
    let parent_revision_id: Option<String> = rusqlite::Row::get(row, 2)
        .map_err(|_| StorageError::database("decode graph revision row"))?;
    let format_version: i64 = rusqlite::Row::get(row, 3)
        .map_err(|_| StorageError::database("decode graph revision row"))?;
    let graph_json: String = rusqlite::Row::get(row, 4)
        .map_err(|_| StorageError::database("decode graph revision row"))?;
    let graph_digest: String = rusqlite::Row::get(row, 5)
        .map_err(|_| StorageError::database("decode graph revision row"))?;
    let source_binding_json: Option<String> = rusqlite::Row::get(row, 6)
        .map_err(|_| StorageError::database("decode graph revision row"))?;
    let package_digests_json: Option<String> = rusqlite::Row::get(row, 7)
        .map_err(|_| StorageError::database("decode graph revision row"))?;
    let compiler_version: Option<String> = rusqlite::Row::get(row, 8)
        .map_err(|_| StorageError::database("decode graph revision row"))?;
    let plan_version_id: Option<String> = rusqlite::Row::get(row, 9)
        .map_err(|_| StorageError::database("decode graph revision row"))?;
    let migration_json: Option<String> = rusqlite::Row::get(row, 10)
        .map_err(|_| StorageError::database("decode graph revision row"))?;
    let created_at_utc: String = rusqlite::Row::get(row, 11)
        .map_err(|_| StorageError::database("decode graph revision row"))?;
    let created_by: String = rusqlite::Row::get(row, 12)
        .map_err(|_| StorageError::database("decode graph revision row"))?;
    Ok(GraphRevisionRecord {
        id: Uuid::parse_str(&id).map_err(|_| StorageError::database("decode revision id"))?,
        workspace_id,
        graph_id,
        revision_number: usize::try_from(revision_number).unwrap_or_default() as u64,
        parent_revision_id: parent_revision_id.and_then(|id| Uuid::parse_str(&id).ok()),
        format_version: u16::try_from(format_version).unwrap_or_default(),
        graph_json,
        graph_digest,
        source_binding: source_binding_json.and_then(|raw| serde_json::from_str(&raw).ok()),
        package_digests: package_digests_json.and_then(|raw| serde_json::from_str(&raw).ok()),
        compiler_version,
        plan_version_id: plan_version_id.and_then(|id| Uuid::parse_str(&id).ok()),
        migration: migration_json.and_then(|raw| serde_json::from_str(&raw).ok()),
        created_at: DateTime::parse_from_rfc3339(&created_at_utc)
            .map(|parsed| parsed.with_timezone(&Utc))
            .map_err(|_| StorageError::database("decode revision timestamp"))?,
        created_by,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ControlPlaneStore;
    use chrono::Utc;

    fn uuid(value: u128) -> Uuid {
        Uuid::from_u128(value)
    }

    fn store() -> (tempfile::TempDir, ControlPlaneStore) {
        let root = tempfile::tempdir().expect("tempdir");
        let store = ControlPlaneStore::open(root.path()).expect("store");
        (root, store)
    }

    fn draft(content: &str) -> RevisionDraft {
        RevisionDraft {
            format_version: 1,
            graph_json: content.to_owned(),
            // Deterministic 64-hex stand-in: the API layer computes the
            // real SHA-256; the store only compares equality and enforces
            // the 64-hex shape.
            graph_digest: {
                let mut digest = String::with_capacity(64);
                for index in 0..32 {
                    let byte = content
                        .as_bytes()
                        .get(index % content.len().max(1))
                        .copied()
                        .unwrap_or(0);
                    digest.push_str(&format!("{byte:02x}"));
                }
                digest
            },
            source_binding: None,
            package_digests: None,
            compiler_version: Some("ng-nodegraph-compiler-v1".to_owned()),
            created_by: "member:00000000-0000-0000-0000-000000000001".to_owned(),
        }
    }

    /// NX-V0 §4.1: first save, CAS, idempotency, and conflict.
    #[test]
    fn save_law_first_cas_idempotent_conflict() {
        let (_root, store) = store();
        let workspace_id = uuid(0xA0);
        let graph_id = uuid(0xA1);
        store
            .create_workspace(workspace_id, Utc::now())
            .expect("workspace");
        let revisions = store.graph_revisions();

        let first = revisions
            .save(workspace_id, graph_id, None, draft("one"))
            .expect("first save");
        let saved = match first {
            GraphRevisionSave::Saved(record) => record,
            GraphRevisionSave::Current(_) => panic!("first save must save"),
        };
        assert_eq!(saved.revision_number, 1);
        assert_eq!(saved.graph_json, "one");
        assert!(saved.parent_revision_id.is_none());

        // Idempotent: the same content is a no-op returning the current.
        let again = revisions
            .save(workspace_id, graph_id, None, draft("one"))
            .expect("idempotent save");
        assert!(matches!(again, GraphRevisionSave::Current(record) if record.revision_number == 1));

        // CAS append.
        let saved_two = revisions
            .save(
                workspace_id,
                graph_id,
                Some(ExpectedRevision {
                    number: 1,
                    digest: Some(saved.graph_digest.clone()),
                }),
                draft("two"),
            )
            .expect("cas save");
        let two = match saved_two {
            GraphRevisionSave::Saved(record) => record,
            GraphRevisionSave::Current(_) => panic!("changed content must save"),
        };
        assert_eq!(two.revision_number, 2);
        assert_eq!(two.parent_revision_id, Some(saved.id));

        // Stale expectation conflicts and names the current revision.
        let error = revisions
            .save(
                workspace_id,
                graph_id,
                Some(ExpectedRevision {
                    number: 1,
                    digest: None,
                }),
                draft("three"),
            )
            .expect_err("stale expectation conflicts");
        assert!(matches!(error, StorageError::AlreadyExists(_)));
        let current = revisions
            .get(workspace_id, graph_id, None)
            .expect("current")
            .expect("current exists");
        assert_eq!(current.revision_number, 2, "the conflict did not write");
        assert_eq!(current.graph_digest.chars().count(), 64);
    }

    /// NX-V0 §4.1/§7: history is append-only, newest first, and survives a
    /// full store reopen (the restart/backing for the HTTP acceptance).
    #[test]
    fn history_is_immutable_and_survives_reopen() {
        let (root, store) = store();
        let workspace_id = uuid(0xB0);
        let graph_id = uuid(0xB1);
        store
            .create_workspace(workspace_id, Utc::now())
            .expect("workspace");
        let revisions = store.graph_revisions();
        revisions
            .save(workspace_id, graph_id, None, draft("v1"))
            .expect("v1");
        revisions
            .save(workspace_id, graph_id, None, draft("v2"))
            .expect("v2");
        revisions
            .save(workspace_id, graph_id, None, draft("v3"))
            .expect("v3");

        let history = revisions
            .history(workspace_id, graph_id, 10)
            .expect("history");
        assert_eq!(history.len(), 3);
        assert_eq!(history[0].graph_json, "v3");
        assert_eq!(history[2].graph_json, "v1");

        // The plan-version link lands on the newest revision only.
        let plan_version_id = uuid(0xB2);
        let linked = revisions
            .link_plan_version(workspace_id, graph_id, plan_version_id)
            .expect("link");
        assert_eq!(linked.revision_number, 3);
        assert_eq!(linked.plan_version_id, Some(plan_version_id));
        let older = revisions
            .get(workspace_id, graph_id, Some(2))
            .expect("older")
            .expect("older exists");
        assert_eq!(older.plan_version_id, None);
        let linked_again = revisions
            .link_plan_version(workspace_id, graph_id, uuid(0xB3))
            .expect("relink");
        assert_eq!(linked_again.plan_version_id, Some(uuid(0xB3)));
        assert_eq!(
            linked_again.graph_json, "v3",
            "content untouched by the link"
        );

        // Full reopen: the revision survives (the store path is the
        // process-restart backing). Every Arc<StoreInner> holder must be
        // gone for the managed-root lock to release.
        drop(revisions);
        drop(store);
        let reopened = ControlPlaneStore::open(root.path()).expect("reopen");
        let restored = reopened
            .graph_revisions()
            .get(workspace_id, graph_id, Some(1))
            .expect("fetch after reopen")
            .expect("revision survives");
        assert_eq!(restored.graph_json, "v1");
        assert_eq!(restored.created_at, history[2].created_at);
    }

    /// NX-V0 §4.2: cross-workspace access finds nothing (indistinguishable
    /// from absent).
    #[test]
    fn revisions_are_workspace_scoped() {
        let (_root, store) = store();
        let workspace_id = uuid(0xC0);
        let other_workspace = uuid(0xC1);
        let graph_id = uuid(0xC2);
        store
            .create_workspace(workspace_id, Utc::now())
            .expect("workspace");
        store
            .create_workspace(other_workspace, Utc::now())
            .expect("workspace");
        let revisions = store.graph_revisions();
        revisions
            .save(workspace_id, graph_id, None, draft("secret"))
            .expect("save");
        assert!(revisions
            .get(other_workspace, graph_id, None)
            .expect("no cross-workspace error")
            .is_none());
        assert!(revisions
            .history(other_workspace, graph_id, 10)
            .expect("no cross-workspace error")
            .is_empty());
    }

    /// NX-V0 §5.3: the migrated append chains from the current tip and
    /// refuses non-current parents; one interrupted apply leaves no partial
    /// state (the insert is single-transaction).
    #[test]
    fn migrated_append_chains_and_refuses_stale_parents() {
        let (_root, store) = store();
        let workspace_id = uuid(0xD0);
        let graph_id = uuid(0xD1);
        store
            .create_workspace(workspace_id, Utc::now())
            .expect("workspace");
        let revisions = store.graph_revisions();
        revisions
            .save(workspace_id, graph_id, None, draft("v1"))
            .expect("v1");
        revisions
            .save(workspace_id, graph_id, None, draft("v2"))
            .expect("v2");
        let stale = revisions
            .get(workspace_id, graph_id, Some(1))
            .expect("stale parent")
            .expect("stale parent exists");
        let error = revisions
            .append_migrated(
                &stale,
                2,
                "migrated".to_owned(),
                "1111111111111111111111111111111111111111111111111111111111111111".to_owned(),
                serde_json::json!({"fromFormat": 1, "toFormat": 2}),
            )
            .expect_err("non-current parent refused");
        assert!(matches!(error, StorageError::AlreadyExists(_)));
        let current = revisions
            .get(workspace_id, graph_id, None)
            .expect("current")
            .expect("current exists");
        assert_eq!(current.revision_number, 2, "no partial state");

        let migrated = revisions
            .append_migrated(
                &current,
                2,
                "migrated".to_owned(),
                "1111111111111111111111111111111111111111111111111111111111111111".to_owned(),
                serde_json::json!({"fromFormat": 1, "toFormat": 2}),
            )
            .expect("append");
        assert_eq!(migrated.revision_number, 3);
        assert_eq!(migrated.format_version, 2);
        assert_eq!(migrated.parent_revision_id, Some(current.id));
        assert_eq!(
            migrated
                .migration
                .as_ref()
                .and_then(|m| m.get("toFormat"))
                .and_then(serde_json::Value::as_u64),
            Some(2)
        );
        // The parent is intact (history never rewritten).
        let parent = revisions
            .get(workspace_id, graph_id, Some(2))
            .expect("parent")
            .expect("parent exists");
        assert_eq!(parent.graph_json, "v2");
        assert_eq!(parent.format_version, 1);
    }
}
