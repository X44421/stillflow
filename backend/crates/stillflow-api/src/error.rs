//! Sanitized, stable API boundary errors.

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type ApiResult<T> = Result<T, ApiError>;

/// Stable error categories exposed by the transport-neutral API boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ApiErrorCode {
    UnsupportedVersion,
    InvalidRequest,
    NotFound,
    Conflict,
    LimitExceeded,
    Unauthorized,
    Internal,
}

/// API error that is safe to serialize to an external caller.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
#[error("{code:?}: {message}")]
#[serde(rename_all = "camelCase")]
pub struct ApiError {
    pub code: ApiErrorCode,
    pub message: String,
}

impl ApiError {
    pub fn new(code: ApiErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn unsupported_version(version: u16) -> Self {
        Self::new(
            ApiErrorCode::UnsupportedVersion,
            format!("unsupported API version {version}"),
        )
    }
}
