use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Connector-specific opaque resume token with version metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Checkpoint {
    pub version: u32,
    pub token: Vec<u8>,
    pub created_at: DateTime<Utc>,
}

impl Checkpoint {
    pub fn new(version: u32, token: Vec<u8>) -> Self {
        Self {
            version,
            token,
            created_at: Utc::now(),
        }
    }
}
