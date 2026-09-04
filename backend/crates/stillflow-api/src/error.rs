//! Sanitized, stable API boundary errors.

use serde::{Deserialize, Serialize};
use stillflow_core::{ConnectorError, ErrorCategory};
use stillflow_engine::{EngineError, JobRuntimeError};
use stillflow_storage::StorageError;
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

    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(ApiErrorCode::InvalidRequest, message)
    }

    pub fn not_found() -> Self {
        Self::new(ApiErrorCode::NotFound, "object was not found")
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(ApiErrorCode::Conflict, message)
    }

    pub fn limit(message: impl Into<String>) -> Self {
        Self::new(ApiErrorCode::LimitExceeded, message)
    }

    pub fn internal() -> Self {
        Self::new(ApiErrorCode::Internal, "internal API error")
    }
}

impl From<StorageError> for ApiError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::NotFound(_) => Self::not_found(),
            StorageError::AlreadyExists(_) | StorageError::ExportDestinationExists(_) => {
                Self::conflict("object already exists or the request conflicts with current state")
            }
            StorageError::Busy(_) => Self::limit("the operation is temporarily busy or bounded"),
            StorageError::EnvelopeLimitExceeded { .. }
            | StorageError::PartitionLimitExceeded { .. }
            | StorageError::RowLimitExceeded { .. }
            | StorageError::StoredByteLimitExceeded { .. }
            | StorageError::ArtifactRowLimitExceeded { .. }
            | StorageError::ArtifactByteLimitExceeded { .. }
            | StorageError::ArtifactPartitionLimitExceeded { .. }
            | StorageError::DedupKeyLimitExceeded { .. }
            | StorageError::ExportLimitExceeded { .. }
            | StorageError::DedupIndexLimitExceeded { .. } => {
                Self::limit("the requested resource exceeds its bound")
            }
            StorageError::InvalidConfiguration(_)
            | StorageError::InvalidDraft(_)
            | StorageError::InvalidTimestampOrder(_)
            | StorageError::InvalidManifest(_)
            | StorageError::Sequence { .. }
            | StorageError::LineageMismatch { .. }
            | StorageError::SchemaDrift { .. }
            | StorageError::UnsupportedStorageVersion(_)
            | StorageError::ExportStagingExists(_)
            | StorageError::ExportNotCommitted(_)
            | StorageError::Snapshot(_)
            | StorageError::IdentityNotFound
            | StorageError::IdentityAlreadyExists
            | StorageError::Identity(_)
            | StorageError::ArithmeticOverflow(_)
            | StorageError::Integrity { .. }
            | StorageError::Io { .. }
            | StorageError::Database(_)
            | StorageError::Parquet(_)
            | StorageError::Serialization(_)
            | StorageError::Batch(_)
            | StorageError::ActivityState => Self::invalid("storage rejected the request"),
        }
    }
}

impl From<EngineError> for ApiError {
    fn from(error: EngineError) -> Self {
        match error.category() {
            ErrorCategory::NotFound => Self::not_found(),
            ErrorCategory::RateLimited => Self::limit("the execution engine is busy or bounded"),
            ErrorCategory::Cancelled => {
                Self::new(ApiErrorCode::Conflict, "operation was cancelled")
            }
            ErrorCategory::Timeout => Self::limit("operation exceeded its time bound"),
            ErrorCategory::InvalidConfiguration
            | ErrorCategory::InvalidData
            | ErrorCategory::SchemaDrift
            | ErrorCategory::UnsupportedCapability
            | ErrorCategory::Authentication
            | ErrorCategory::Authorization
            | ErrorCategory::TransientSource => Self::invalid("engine rejected the request"),
            ErrorCategory::Internal => Self::internal(),
        }
    }
}

impl From<ConnectorError> for ApiError {
    fn from(error: ConnectorError) -> Self {
        match error.category() {
            ErrorCategory::NotFound => Self::not_found(),
            ErrorCategory::RateLimited | ErrorCategory::Timeout => {
                Self::limit("connector operation exceeded its bound")
            }
            ErrorCategory::Authentication | ErrorCategory::Authorization => {
                Self::new(ApiErrorCode::Unauthorized, "connector authorization failed")
            }
            ErrorCategory::Internal => Self::internal(),
            _ => Self::invalid("connector rejected the request"),
        }
    }
}

impl From<JobRuntimeError> for ApiError {
    fn from(error: JobRuntimeError) -> Self {
        match error {
            JobRuntimeError::Storage(inner) => inner.into(),
            JobRuntimeError::Engine(inner) => inner.into(),
            JobRuntimeError::Invalid(_) => Self::invalid("job runtime rejected the request"),
            JobRuntimeError::Shutdown => Self::conflict("job runtime is shutting down"),
            JobRuntimeError::ResolverTimeout => Self::limit("job resolution exceeded its bound"),
            JobRuntimeError::ResolverPanic | JobRuntimeError::WorkerPanic => Self::internal(),
        }
    }
}
