use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Stable connector error categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ErrorCategory {
    Authentication,
    Authorization,
    NotFound,
    InvalidConfiguration,
    InvalidData,
    SchemaDrift,
    RateLimited,
    Timeout,
    Cancelled,
    UnsupportedCapability,
    TransientSource,
    Internal,
}

/// Sanitized error summary safe for events and API responses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SanitizedErrorSummary {
    pub category: ErrorCategory,
    pub retryable: bool,
    pub message: String,
}

/// Typed connector failure with retryability and sanitized context.
#[derive(Debug, Error)]
#[error("{user_message}")]
pub struct ConnectorError {
    category: ErrorCategory,
    retryable: bool,
    user_message: String,
    internal_chain: Vec<String>,
    source_context: BTreeMap<String, String>,
    unsupported_capability: Option<String>,
}

pub type ConnectorResult<T> = Result<T, ConnectorError>;

impl ConnectorError {
    pub fn category(&self) -> ErrorCategory {
        self.category
    }

    pub fn retryable(&self) -> bool {
        self.retryable
    }

    pub fn user_message(&self) -> &str {
        &self.user_message
    }

    pub fn internal_chain(&self) -> &[String] {
        &self.internal_chain
    }

    pub fn source_context(&self) -> &BTreeMap<String, String> {
        &self.source_context
    }

    pub fn missing_capability(&self) -> Option<&str> {
        self.unsupported_capability.as_deref()
    }

    pub fn sanitized_summary(&self) -> SanitizedErrorSummary {
        SanitizedErrorSummary {
            category: self.category,
            retryable: self.retryable,
            message: self.user_message.clone(),
        }
    }

    pub fn for_unsupported_capability(capability: impl Into<String>) -> Self {
        let capability = capability.into();
        Self {
            category: ErrorCategory::UnsupportedCapability,
            retryable: false,
            user_message: format!("connector does not support capability: {capability}"),
            internal_chain: Vec::new(),
            source_context: BTreeMap::new(),
            unsupported_capability: Some(capability),
        }
    }

    pub fn cancelled() -> Self {
        Self {
            category: ErrorCategory::Cancelled,
            retryable: false,
            user_message: "operation was cancelled".to_owned(),
            internal_chain: Vec::new(),
            source_context: BTreeMap::new(),
            unsupported_capability: None,
        }
    }

    pub fn timeout(message: impl Into<String>) -> Self {
        Self {
            category: ErrorCategory::Timeout,
            retryable: true,
            user_message: sanitize_message(message.into()),
            internal_chain: Vec::new(),
            source_context: BTreeMap::new(),
            unsupported_capability: None,
        }
    }

    pub fn invalid_configuration(message: impl Into<String>) -> Self {
        Self::with_category(
            ErrorCategory::InvalidConfiguration,
            false,
            message,
            Vec::new(),
            BTreeMap::new(),
        )
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::with_category(
            ErrorCategory::Internal,
            false,
            message,
            Vec::new(),
            BTreeMap::new(),
        )
    }

    pub fn with_category(
        category: ErrorCategory,
        retryable: bool,
        user_message: impl Into<String>,
        internal_chain: Vec<String>,
        source_context: BTreeMap<String, String>,
    ) -> Self {
        let sanitized_context = source_context
            .into_iter()
            .map(|(key, value)| (key, sanitize_message(value)))
            .collect();
        Self {
            category,
            retryable,
            user_message: sanitize_message(user_message.into()),
            internal_chain,
            source_context: sanitized_context,
            unsupported_capability: None,
        }
    }
}

const SECRET_FIELD_NAMES: &[&str] = &[
    "password",
    "secret",
    "token",
    "api_key",
    "apikey",
    "access_key",
    "connection_string",
];

/// Rejects JSON objects that contain known secret field names.
pub fn ensure_no_secret_fields(value: &serde_json::Value) -> ConnectorResult<()> {
    match value {
        serde_json::Value::Object(map) => {
            for key in map.keys() {
                let normalized = key.to_ascii_lowercase();
                if SECRET_FIELD_NAMES
                    .iter()
                    .any(|candidate| normalized.contains(candidate))
                {
                    return Err(ConnectorError::invalid_configuration(format!(
            "configuration must not embed secret field `{key}`; use credential references instead"
          )));
                }
                ensure_no_secret_fields(&map[key])?;
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                ensure_no_secret_fields(item)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Redacts common secret patterns from user-visible messages.
pub fn sanitize_message(message: String) -> String {
    let mut sanitized = message;
    for marker in ["password=", "token=", "api_key=", "secret="] {
        if let Some(index) = sanitized.to_ascii_lowercase().find(marker) {
            let start = index + marker.len();
            let end = sanitized[start..]
                .find(|character: char| character.is_whitespace() || character == ';')
                .map(|offset| start + offset)
                .unwrap_or(sanitized.len());
            sanitized.replace_range(start..end, "***");
        }
    }
    sanitized
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitizes_password_fragments() {
        let message = sanitize_message("failed: password=hunter2; host=db".to_owned());
        assert!(message.contains("password=***"));
        assert!(!message.contains("hunter2"));
    }

    #[test]
    fn rejects_nested_secret_fields() {
        let config = serde_json::json!({
          "options": { "apiKey": "abc" }
        });
        ensure_no_secret_fields(&config).expect_err("nested secret keys must be rejected");
    }

    #[test]
    fn unsupported_capability_is_not_retryable() {
        let error = ConnectorError::for_unsupported_capability("predicate_pushdown");
        assert_eq!(error.category(), ErrorCategory::UnsupportedCapability);
        assert!(!error.retryable());
        assert_eq!(error.missing_capability(), Some("predicate_pushdown"));
    }
}
