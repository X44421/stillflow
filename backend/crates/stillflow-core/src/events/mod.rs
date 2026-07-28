use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::SanitizedErrorSummary;

/// Connector implementation kind used by the registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ConnectorKind {
    LocalFile,
    ObjectStore,
    SqlDatabase,
    ExcelWorkbook,
    DocumentWorker,
}

/// DataCleaner OS object kinds participating in ingestion events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ObjectKind {
    Session,
    SourceConnection,
    SourceAsset,
    Dataset,
    Snapshot,
    Capability,
}

/// Relationship or lifecycle transition recorded on an object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RelationshipKind {
    ConnectsTo,
    Contains,
    Discovered,
    Inspected,
    Imported,
    Profiled,
    SnapshotOf,
    Materialized,
    Restored,
    Reads,
    Produces,
    Checkpointed,
    Completed,
    Tested,
    Failed,
}

/// Auditable ingestion event with sanitized metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IngestionEvent {
    pub id: Uuid,
    pub session_id: Uuid,
    pub object_kind: ObjectKind,
    pub object_id: Uuid,
    pub relationship: RelationshipKind,
    pub timestamp: DateTime<Utc>,
    pub metadata: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<SanitizedErrorSummary>,
}

impl IngestionEvent {
    pub fn new(
        session_id: Uuid,
        object_kind: ObjectKind,
        object_id: Uuid,
        relationship: RelationshipKind,
        metadata: serde_json::Value,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            session_id,
            object_kind,
            object_id,
            relationship,
            timestamp: Utc::now(),
            metadata,
            error: None,
        }
    }

    pub fn with_error(mut self, error: SanitizedErrorSummary) -> Self {
        self.error = Some(error);
        self
    }
}

/// Maps ingestion concepts to DataCleaner OS objects and events.
pub struct ObjectEventMapper;

impl ObjectEventMapper {
    pub fn connection_tested(session_id: Uuid, connection_id: Uuid) -> IngestionEvent {
        IngestionEvent::new(
            session_id,
            ObjectKind::SourceConnection,
            connection_id,
            RelationshipKind::Tested,
            serde_json::json!({ "status": "ok" }),
        )
    }

    pub fn connection_failed(
        session_id: Uuid,
        connection_id: Uuid,
        error: SanitizedErrorSummary,
    ) -> IngestionEvent {
        IngestionEvent::new(
            session_id,
            ObjectKind::SourceConnection,
            connection_id,
            RelationshipKind::Failed,
            serde_json::json!({}),
        )
        .with_error(error)
    }

    pub fn asset_discovered(session_id: Uuid, asset_id: Uuid, name: &str) -> IngestionEvent {
        IngestionEvent::new(
            session_id,
            ObjectKind::SourceAsset,
            asset_id,
            RelationshipKind::Discovered,
            serde_json::json!({ "name": name }),
        )
    }

    pub fn asset_inspected(session_id: Uuid, asset_id: Uuid, format: &str) -> IngestionEvent {
        IngestionEvent::new(
            session_id,
            ObjectKind::SourceAsset,
            asset_id,
            RelationshipKind::Inspected,
            serde_json::json!({ "format": format }),
        )
    }

    pub fn dataset_imported(session_id: Uuid, dataset_id: Uuid) -> IngestionEvent {
        IngestionEvent::new(
            session_id,
            ObjectKind::Dataset,
            dataset_id,
            RelationshipKind::Imported,
            serde_json::json!({}),
        )
    }

    pub fn snapshot_materialized(session_id: Uuid, snapshot_id: Uuid) -> IngestionEvent {
        IngestionEvent::new(
            session_id,
            ObjectKind::Snapshot,
            snapshot_id,
            RelationshipKind::Materialized,
            serde_json::json!({}),
        )
    }

    pub fn session_completed(session_id: Uuid) -> IngestionEvent {
        IngestionEvent::new(
            session_id,
            ObjectKind::Session,
            session_id,
            RelationshipKind::Completed,
            serde_json::json!({}),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{ConnectorError, ErrorCategory};

    #[test]
    fn event_mapping_covers_core_objects() {
        let session_id = Uuid::new_v4();
        let connection_id = Uuid::new_v4();
        let asset_id = Uuid::new_v4();
        let dataset_id = Uuid::new_v4();
        let snapshot_id = Uuid::new_v4();

        let events = [
            ObjectEventMapper::connection_tested(session_id, connection_id),
            ObjectEventMapper::asset_discovered(session_id, asset_id, "orders.csv"),
            ObjectEventMapper::dataset_imported(session_id, dataset_id),
            ObjectEventMapper::snapshot_materialized(session_id, snapshot_id),
            ObjectEventMapper::session_completed(session_id),
        ];

        assert_eq!(events[0].object_kind, ObjectKind::SourceConnection);
        assert_eq!(events[1].relationship, RelationshipKind::Discovered);
        assert_eq!(events[4].relationship, RelationshipKind::Completed);
    }

    #[test]
    fn failed_event_carries_sanitized_error_only() {
        let session_id = Uuid::new_v4();
        let connection_id = Uuid::new_v4();
        let error = ConnectorError::with_category(
            ErrorCategory::Authentication,
            false,
            "password=secret-value",
            vec!["internal detail".to_owned()],
            Default::default(),
        )
        .sanitized_summary();
        let event = ObjectEventMapper::connection_failed(session_id, connection_id, error);
        let json = serde_json::to_string(&event).expect("serialize event");
        assert!(!json.contains("secret-value"));
        assert!(!json.contains("internal detail"));
        assert!(json.contains("authentication"));
    }
}
