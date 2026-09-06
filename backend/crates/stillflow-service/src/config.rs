//! Process-level configuration. The frozen `ServiceConfig` is consumed
//! verbatim (flattened); the process adds only the authorization-mode selector
//! and the single workspace it serves (contract §4.2/§4.3).

use serde::Deserialize;
use uuid::Uuid;

use stillflow_api::ServiceConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AuthModeConfig {
    #[default]
    LocalTrusted,
    Server,
}

impl From<AuthModeConfig> for stillflow_api::AuthorizationMode {
    fn from(value: AuthModeConfig) -> Self {
        match value {
            AuthModeConfig::LocalTrusted => stillflow_api::AuthorizationMode::LocalTrusted,
            AuthModeConfig::Server => stillflow_api::AuthorizationMode::Server,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessConfig {
    #[serde(flatten)]
    pub service: ServiceConfig,
    #[serde(default)]
    pub authorization_mode: AuthModeConfig,
    pub workspace_id: Uuid,
}
