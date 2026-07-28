use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Root runtime object for an ingestion workflow.
///
/// A session may reference multiple [`SourceConnection`] objects while
/// orchestrating imports across sources.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Session {
    pub id: Uuid,
    pub connection_ids: Vec<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Session {
    pub fn new(connection_ids: Vec<Uuid>) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            connection_ids,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn with_connection(connection_id: Uuid) -> Self {
        Self::new(vec![connection_id])
    }

    pub fn add_connection(mut self, connection_id: Uuid) -> Self {
        if !self.connection_ids.contains(&connection_id) {
            self.connection_ids.push(connection_id);
            self.updated_at = Utc::now();
        }
        self
    }

    pub fn primary_connection_id(&self) -> Option<Uuid> {
        self.connection_ids.first().copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supports_multiple_connections() {
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        let session = Session::with_connection(first).add_connection(second);
        assert_eq!(session.connection_ids, vec![first, second]);
    }
}
