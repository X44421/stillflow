use chrono::{DateTime, Utc};
use serde::{de::Error as DeError, Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

use crate::error::{ensure_safe_event_metadata, ConnectorResult, SanitizedErrorSummary};

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
#[derive(Debug, Clone, PartialEq)]
pub struct IngestionEvent {
    id: Uuid,
    session_id: Uuid,
    object_kind: ObjectKind,
    object_id: Uuid,
    relationship: RelationshipKind,
    timestamp: DateTime<Utc>,
    metadata: serde_json::Value,
    error: Option<SanitizedErrorSummary>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IngestionEventData {
    id: Uuid,
    session_id: Uuid,
    object_kind: ObjectKind,
    object_id: Uuid,
    relationship: RelationshipKind,
    timestamp: DateTime<Utc>,
    metadata: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<SanitizedErrorSummary>,
}

impl Serialize for IngestionEvent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        IngestionEventData {
            id: self.id,
            session_id: self.session_id,
            object_kind: self.object_kind,
            object_id: self.object_id,
            relationship: self.relationship,
            timestamp: self.timestamp,
            metadata: self.metadata.clone(),
            error: self.error.clone(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for IngestionEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let data = IngestionEventData::deserialize(deserializer)?;
        ensure_safe_event_metadata(&data.metadata).map_err(DeError::custom)?;
        Ok(Self {
            id: data.id,
            session_id: data.session_id,
            object_kind: data.object_kind,
            object_id: data.object_id,
            relationship: data.relationship,
            timestamp: data.timestamp,
            metadata: data.metadata,
            error: data.error,
        })
    }
}

impl IngestionEvent {
    pub fn try_new(
        session_id: Uuid,
        object_kind: ObjectKind,
        object_id: Uuid,
        relationship: RelationshipKind,
        metadata: serde_json::Value,
    ) -> ConnectorResult<Self> {
        ensure_safe_event_metadata(&metadata)?;
        Ok(Self {
            id: Uuid::new_v4(),
            session_id,
            object_kind,
            object_id,
            relationship,
            timestamp: Utc::now(),
            metadata,
            error: None,
        })
    }

    pub fn with_error(mut self, error: SanitizedErrorSummary) -> Self {
        self.error = Some(error);
        self
    }

    pub fn try_with_error(
        mut self,
        category: crate::ErrorCategory,
        retryable: bool,
        message: impl Into<String>,
    ) -> ConnectorResult<Self> {
        self.error = Some(SanitizedErrorSummary::try_new(
            category, retryable, message,
        )?);
        Ok(self)
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn session_id(&self) -> Uuid {
        self.session_id
    }

    pub fn metadata(&self) -> &serde_json::Value {
        &self.metadata
    }

    pub fn object_kind(&self) -> ObjectKind {
        self.object_kind
    }

    pub fn relationship(&self) -> RelationshipKind {
        self.relationship
    }

    pub fn error(&self) -> Option<&SanitizedErrorSummary> {
        self.error.as_ref()
    }
}

/// Maps ingestion concepts to DataCleaner OS objects and events.
pub struct ObjectEventMapper;

impl ObjectEventMapper {
    pub fn connection_tested(
        session_id: Uuid,
        connection_id: Uuid,
    ) -> ConnectorResult<IngestionEvent> {
        IngestionEvent::try_new(
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
    ) -> ConnectorResult<IngestionEvent> {
        Ok(IngestionEvent::try_new(
            session_id,
            ObjectKind::SourceConnection,
            connection_id,
            RelationshipKind::Failed,
            serde_json::json!({}),
        )?
        .with_error(error))
    }

    pub fn asset_discovered(
        session_id: Uuid,
        asset_id: Uuid,
        name: &str,
    ) -> ConnectorResult<IngestionEvent> {
        IngestionEvent::try_new(
            session_id,
            ObjectKind::SourceAsset,
            asset_id,
            RelationshipKind::Discovered,
            serde_json::json!({ "name": name }),
        )
    }

    pub fn asset_inspected(
        session_id: Uuid,
        asset_id: Uuid,
        format: &str,
    ) -> ConnectorResult<IngestionEvent> {
        IngestionEvent::try_new(
            session_id,
            ObjectKind::SourceAsset,
            asset_id,
            RelationshipKind::Inspected,
            serde_json::json!({ "format": format }),
        )
    }

    pub fn dataset_imported(session_id: Uuid, dataset_id: Uuid) -> ConnectorResult<IngestionEvent> {
        IngestionEvent::try_new(
            session_id,
            ObjectKind::Dataset,
            dataset_id,
            RelationshipKind::Imported,
            serde_json::json!({}),
        )
    }

    pub fn snapshot_materialized(
        session_id: Uuid,
        snapshot_id: Uuid,
    ) -> ConnectorResult<IngestionEvent> {
        IngestionEvent::try_new(
            session_id,
            ObjectKind::Snapshot,
            snapshot_id,
            RelationshipKind::Materialized,
            serde_json::json!({}),
        )
    }

    pub fn session_completed(session_id: Uuid) -> ConnectorResult<IngestionEvent> {
        IngestionEvent::try_new(
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
            ObjectEventMapper::connection_tested(session_id, connection_id).expect("event"),
            ObjectEventMapper::asset_discovered(session_id, asset_id, "orders.csv").expect("event"),
            ObjectEventMapper::dataset_imported(session_id, dataset_id).expect("event"),
            ObjectEventMapper::snapshot_materialized(session_id, snapshot_id).expect("event"),
            ObjectEventMapper::session_completed(session_id).expect("event"),
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
        let event =
            ObjectEventMapper::connection_failed(session_id, connection_id, error).expect("event");
        let json = serde_json::to_string(&event).expect("serialize event");
        assert!(!json.contains("secret-value"));
        assert!(!json.contains("internal detail"));
        assert!(json.contains("authentication"));
    }

    #[test]
    fn sanitizes_secret_error_on_deserialize() {
        let json = serde_json::json!({
            "id": Uuid::new_v4(),
            "sessionId": Uuid::new_v4(),
            "objectKind": "sourceConnection",
            "objectId": Uuid::new_v4(),
            "relationship": "failed",
            "timestamp": Utc::now(),
            "metadata": {},
            "error": {
                "category": "authentication",
                "retryable": false,
                "message": "password=secret-value"
            }
        });
        let event: IngestionEvent = serde_json::from_value(json).expect("deserialize");
        let error = event.error().expect("error");
        assert!(!error.message().contains("secret-value"));
        assert!(error.message().contains("password=***"));
    }

    #[test]
    fn rejects_secret_metadata_on_deserialize() {
        let json = serde_json::json!({
            "id": Uuid::new_v4(),
            "sessionId": Uuid::new_v4(),
            "objectKind": "sourceConnection",
            "objectId": Uuid::new_v4(),
            "relationship": "failed",
            "timestamp": Utc::now(),
            "metadata": { "password": "secret" }
        });
        serde_json::from_value::<IngestionEvent>(json).expect_err("metadata must be validated");
    }
}
