use std::fmt;

use async_trait::async_trait;
use stillflow_core::{ConnectorError, ConnectorResult, CredentialRef};

/// Ephemeral S3 credentials resolved inside the Stillflow server boundary.
pub struct S3CredentialMaterial {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
}

impl S3CredentialMaterial {
    pub fn new(
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        session_token: Option<String>,
    ) -> ConnectorResult<Self> {
        let material = Self {
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
            session_token,
        };
        if material.access_key_id.is_empty() || material.secret_access_key.is_empty() {
            return Err(ConnectorError::invalid_configuration(
                "resolved S3 credentials are incomplete",
            ));
        }
        Ok(material)
    }

    pub(crate) fn take_parts(mut self) -> (String, String, Option<String>) {
        (
            std::mem::take(&mut self.access_key_id),
            std::mem::take(&mut self.secret_access_key),
            self.session_token.take(),
        )
    }
}

impl fmt::Debug for S3CredentialMaterial {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("S3CredentialMaterial")
            .field("access_key_id", &"[REDACTED]")
            .field("secret_access_key", &"[REDACTED]")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

/// Resolves an opaque credential reference without exposing secret values to
/// the browser-facing domain model.
#[async_trait]
pub trait ObjectStoreCredentialResolver: Send + Sync {
    async fn resolve_s3(
        &self,
        credential_ref: &CredentialRef,
    ) -> ConnectorResult<S3CredentialMaterial>;
}

#[derive(Debug, Default)]
pub(crate) struct RejectingCredentialResolver;

#[async_trait]
impl ObjectStoreCredentialResolver for RejectingCredentialResolver {
    async fn resolve_s3(
        &self,
        _credential_ref: &CredentialRef,
    ) -> ConnectorResult<S3CredentialMaterial> {
        Err(ConnectorError::with_category(
            stillflow_core::ErrorCategory::Authentication,
            false,
            "object storage credentials are unavailable",
            Vec::new(),
            std::collections::BTreeMap::new(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_output_redacts_every_secret_value() {
        let material = S3CredentialMaterial::new(
            "SENTINEL_ACCESS",
            "SENTINEL_SECRET",
            Some("SENTINEL_TOKEN".to_owned()),
        )
        .expect("material");
        let debug = format!("{material:?}");
        assert!(!debug.contains("SENTINEL"));
        assert_eq!(debug.matches("[REDACTED]").count(), 3);
    }
}
