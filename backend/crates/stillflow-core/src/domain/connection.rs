use chrono::{DateTime, Utc};
use serde::{de::Error as DeError, Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

use crate::error::{ensure_no_secret_fields, ConnectorResult};
use crate::events::ConnectorKind;

/// Reference to credentials stored outside the domain model.
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
#[derive(Debug, Clone, PartialEq)]
pub struct SourceConnection {
    id: Uuid,
    kind: ConnectorKind,
    name: String,
    config: serde_json::Value,
    credential_ref: CredentialRef,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SourceConnectionData {
    id: Uuid,
    kind: ConnectorKind,
    name: String,
    config: serde_json::Value,
    credential_ref: CredentialRef,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl Serialize for SourceConnection {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        SourceConnectionData {
            id: self.id,
            kind: self.kind,
            name: self.name.clone(),
            config: self.config.clone(),
            credential_ref: self.credential_ref.clone(),
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SourceConnection {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let data = SourceConnectionData::deserialize(deserializer)?;
        ensure_no_secret_fields(&data.config).map_err(DeError::custom)?;
        Ok(Self {
            id: data.id,
            kind: data.kind,
            name: data.name,
            config: data.config,
            credential_ref: data.credential_ref,
            created_at: data.created_at,
            updated_at: data.updated_at,
        })
    }
}

impl SourceConnection {
    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn kind(&self) -> ConnectorKind {
        self.kind
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn config(&self) -> &serde_json::Value {
        &self.config
    }

    pub fn credential_ref(&self) -> &CredentialRef {
        &self.credential_ref
    }

    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    pub fn validate(&self) -> ConnectorResult<()> {
        ensure_no_secret_fields(&self.config)
    }

    pub fn try_new(
        kind: ConnectorKind,
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
    fn rejects_secret_fields_on_deserialize() {
        let json = serde_json::json!({
            "id": Uuid::new_v4(),
            "kind": "localFile",
            "name": "warehouse",
            "config": { "password": "must-not-persist" },
            "credentialRef": "cred://vault/warehouse",
            "createdAt": Utc::now(),
            "updatedAt": Utc::now()
        });
        let error = serde_json::from_value::<SourceConnection>(json).expect_err("deserialize");
        assert!(error.to_string().contains("secret field"));
    }

    #[test]
    fn rejects_struct_literal_bypass() {
        // SourceConnection fields are private; only validated constructors/deserialize are available.
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
