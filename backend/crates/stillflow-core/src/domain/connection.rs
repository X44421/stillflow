use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{ensure_no_secret_fields, ConnectorResult};

/// Reference to credentials stored outside the domain model.
///
/// Domain objects persist and serialize only the reference handle, never the
/// secret value itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CredentialRef(pub String);

impl CredentialRef {
    pub fn new(reference: impl Into<String>) -> Self {
        Self(reference.into())
    }
}

/// A configured data source endpoint.
///
/// Secrets are never embedded: only a [`CredentialRef`] and non-sensitive
/// configuration are stored or serialized.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceConnection {
    pub id: Uuid,
    pub kind: crate::events::ConnectorKind,
    pub name: String,
    pub config: serde_json::Value,
    pub credential_ref: CredentialRef,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl SourceConnection {
    /// Validates that configuration contains no secret field names.
    pub fn validate(&self) -> ConnectorResult<()> {
        ensure_no_secret_fields(&self.config)?;
        Ok(())
    }

    /// Builds a connection while rejecting configs that embed secret keys.
    pub fn try_new(
        kind: crate::events::ConnectorKind,
        name: impl Into<String>,
        config: serde_json::Value,
        credential_ref: CredentialRef,
    ) -> ConnectorResult<Self> {
        ensure_no_secret_fields(&config)?;
        let now = Utc::now();
        Ok(Self {
            id: Uuid::new_v4(),
            kind,
            name: name.into(),
            config,
            credential_ref,
            created_at: now,
            updated_at: now,
        })
    }
}

/// Outcome of a connector connection test.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ConnectionStatus {
    Ok,
    Degraded { warnings: Vec<String> },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::ConnectorKind;

    #[test]
    fn rejects_secret_fields_in_config() {
        let config = serde_json::json!({
            "host": "db.example.com",
            "password": "must-not-persist"
        });
        let error = SourceConnection::try_new(
            ConnectorKind::SqlDatabase,
            "warehouse",
            config,
            CredentialRef::new("cred://vault/warehouse"),
        )
        .expect_err("secret keys must be rejected");
        assert_eq!(error.category(), crate::ErrorCategory::InvalidConfiguration);
    }

    #[test]
    fn serializes_without_secret_values() {
        let connection = SourceConnection::try_new(
            ConnectorKind::LocalFile,
            "uploads",
            serde_json::json!({ "root": "/data/uploads" }),
            CredentialRef::new("cred://local/default"),
        )
        .expect("valid connection");
        let json = serde_json::to_string(&connection).expect("serialize");
        assert!(!json.contains("password"));
        assert!(json.contains("credentialRef"));
    }
}
