use std::sync::Arc;

use arrow_schema::Schema;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Immutable materialized output plus lineage and quality metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatasetSnapshot {
    pub id: Uuid,
    pub dataset_id: Uuid,
    pub session_id: Uuid,
    pub storage_ref: String,
    pub row_count: u64,
    pub quality_score: Option<u8>,
    pub lineage: Vec<Uuid>,
    #[serde(skip)]
    pub schema: Option<Arc<Schema>>,
    pub created_at: DateTime<Utc>,
}

impl DatasetSnapshot {
    pub fn new(
        dataset_id: Uuid,
        session_id: Uuid,
        storage_ref: impl Into<String>,
        row_count: u64,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            dataset_id,
            session_id,
            storage_ref: storage_ref.into(),
            row_count,
            quality_score: None,
            lineage: Vec::new(),
            schema: None,
            created_at: Utc::now(),
        }
    }
}
