use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Logical imported dataset registered in a session.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Dataset {
    pub id: Uuid,
    pub session_id: Uuid,
    pub source_asset_id: Uuid,
    pub name: String,
    pub created_at: DateTime<Utc>,
}

impl Dataset {
    pub fn new(session_id: Uuid, source_asset_id: Uuid, name: impl Into<String>) -> Self {
        Self {
            id: Uuid::new_v4(),
            session_id,
            source_asset_id,
            name: name.into(),
            created_at: Utc::now(),
        }
    }
}
