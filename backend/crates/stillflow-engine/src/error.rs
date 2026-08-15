use std::fmt;
use std::time::Duration;

use stillflow_core::{ColumnId, ConnectorError, ErrorCategory, SanitizedErrorSummary};
use stillflow_plan::PlanNodeKind;
use stillflow_storage::StorageError;
use thiserror::Error;
use uuid::Uuid;

use crate::{MAX_ENGINE_PEAK_BYTES, MAX_LIVE_COLUMNAR_PAYLOADS};

#[derive(Error)]
pub enum EngineError {
    #[error("operator `{kind}` is not supported in this engine phase")]
    UnsupportedOperator { node: Uuid, kind: &'static str },
    #[error("rule `{kind}` is not supported in this engine phase")]
    UnsupportedRule { node: Uuid, kind: &'static str },
    #[error("connector kind is not supported in this engine phase")]
    UnsupportedCapability { kind: &'static str },
    #[error("scan source binding is invalid")]
    SourceBinding,
    #[error("{0}")]
    InvalidPlan(&'static str),
    #[error("unknown column {0}")]
    UnknownColumn(ColumnId),
    #[error("{0}")]
    TypeError(&'static str),
    #[error("cast failed for column {column} at batch {sequence} row {row}")]
    CastFailure {
        column: ColumnId,
        sequence: u64,
        row: usize,
    },
    #[error("arithmetic failed for column {column} at batch {sequence} row {row}")]
    Arithmetic {
        column: ColumnId,
        sequence: u64,
        row: usize,
    },
    #[error("connector envelope schema drifted from the authorized source schema")]
    SchemaDrift { sequence: u64 },
    #[error("{0}")]
    BoundExceeded(&'static str),
    #[error("Arrow and Polars FFI transfer failed")]
    Ffi,
    #[error("engine run was cancelled")]
    Cancelled,
    #[error("engine run exceeded its deadline")]
    Timeout,
    #[error("engine is busy")]
    Busy,
    #[error("engine internal invariant failed")]
    Internal(&'static str),
    #[error("{0}")]
    Connector(ConnectorError),
    #[error("{0}")]
    Storage(StorageError),
}

impl EngineError {
    pub fn category(&self) -> ErrorCategory {
        match self {
            Self::UnsupportedOperator { .. }
            | Self::UnsupportedRule { .. }
            | Self::UnsupportedCapability { .. } => ErrorCategory::UnsupportedCapability,
            Self::SourceBinding | Self::InvalidPlan(_) | Self::UnknownColumn(_) => {
                ErrorCategory::InvalidConfiguration
            }
            Self::TypeError(_)
            | Self::CastFailure { .. }
            | Self::Arithmetic { .. }
            | Self::BoundExceeded(_) => ErrorCategory::InvalidData,
            Self::SchemaDrift { .. } => ErrorCategory::SchemaDrift,
            Self::Ffi | Self::Internal(_) => ErrorCategory::Internal,
            Self::Cancelled => ErrorCategory::Cancelled,
            Self::Timeout => ErrorCategory::Timeout,
            Self::Busy => ErrorCategory::RateLimited,
            Self::Connector(inner) => inner.category(),
            Self::Storage(inner) => storage_category(inner),
        }
    }

    pub fn retryable(&self) -> bool {
        match self {
            Self::Timeout | Self::Busy => true,
            Self::Connector(inner) => inner.retryable(),
            Self::Storage(inner) => storage_retryable(inner),
            _ => false,
        }
    }

    pub fn sanitized_summary(&self) -> SanitizedErrorSummary {
        let message = self.to_string();
        match SanitizedErrorSummary::try_new(self.category(), self.retryable(), message) {
            Ok(summary) => summary,
            Err(_) => fallback_summary(),
        }
    }

    pub(crate) fn from_connector(error: ConnectorError) -> Self {
        match error.category() {
            ErrorCategory::Cancelled => Self::Cancelled,
            ErrorCategory::Timeout => Self::Timeout,
            _ => Self::Connector(error),
        }
    }

    pub(crate) fn from_storage(error: StorageError) -> Self {
        Self::Storage(error)
    }

    pub(crate) fn unsupported_operator(
        node: stillflow_plan::PlanNodeId,
        kind: &PlanNodeKind,
    ) -> Self {
        Self::UnsupportedOperator {
            node: node.as_uuid(),
            kind: operator_kind_name(kind),
        }
    }

    pub(crate) fn peak_exceeded() -> Self {
        Self::BoundExceeded("engine live columnar payloads or peak bytes exceeded")
    }
}

impl fmt::Debug for EngineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedOperator { node, kind } => formatter
                .debug_struct("UnsupportedOperator")
                .field("node", node)
                .field("kind", kind)
                .finish(),
            Self::UnsupportedRule { node, kind } => formatter
                .debug_struct("UnsupportedRule")
                .field("node", node)
                .field("kind", kind)
                .finish(),
            Self::UnsupportedCapability { kind } => formatter
                .debug_struct("UnsupportedCapability")
                .field("kind", kind)
                .finish(),
            Self::SourceBinding => formatter.debug_struct("SourceBinding").finish(),
            Self::InvalidPlan(reason) => {
                formatter.debug_tuple("InvalidPlan").field(reason).finish()
            }
            Self::UnknownColumn(column) => formatter
                .debug_tuple("UnknownColumn")
                .field(column)
                .finish(),
            Self::TypeError(reason) => formatter.debug_tuple("TypeError").field(reason).finish(),
            Self::CastFailure {
                column,
                sequence,
                row,
            } => formatter
                .debug_struct("CastFailure")
                .field("column", column)
                .field("sequence", sequence)
                .field("row", row)
                .finish(),
            Self::Arithmetic {
                column,
                sequence,
                row,
            } => formatter
                .debug_struct("Arithmetic")
                .field("column", column)
                .field("sequence", sequence)
                .field("row", row)
                .finish(),
            Self::SchemaDrift { sequence } => formatter
                .debug_struct("SchemaDrift")
                .field("sequence", sequence)
                .finish(),
            Self::BoundExceeded(reason) => formatter
                .debug_tuple("BoundExceeded")
                .field(reason)
                .finish(),
            Self::Ffi => formatter.debug_struct("Ffi").finish(),
            Self::Cancelled => formatter.debug_struct("Cancelled").finish(),
            Self::Timeout => formatter.debug_struct("Timeout").finish(),
            Self::Busy => formatter.debug_struct("Busy").finish(),
            Self::Internal(reason) => formatter.debug_tuple("Internal").field(reason).finish(),
            Self::Connector(inner) => formatter
                .debug_struct("Connector")
                .field("category", &inner.category())
                .field("retryable", &inner.retryable())
                .finish(),
            Self::Storage(inner) => formatter
                .debug_struct("Storage")
                .field("category", &storage_category(inner))
                .field("retryable", &storage_retryable(inner))
                .finish(),
        }
    }
}

fn fallback_summary() -> SanitizedErrorSummary {
    SanitizedErrorSummary::try_new(ErrorCategory::Internal, false, "internal error").unwrap_or_else(
        |_| stillflow_core::ConnectorError::internal("internal error").sanitized_summary(),
    )
}

fn operator_kind_name(kind: &PlanNodeKind) -> &'static str {
    match kind {
        PlanNodeKind::Scan { .. } => "scan",
        PlanNodeKind::Project { .. } => "project",
        PlanNodeKind::Filter { .. } => "filter",
        PlanNodeKind::ApplyRules { .. } => "applyRules",
        PlanNodeKind::Join { .. } => "join",
        PlanNodeKind::Union => "union",
        PlanNodeKind::Materialize { .. } => "materialize",
    }
}

fn storage_category(error: &StorageError) -> ErrorCategory {
    match error {
        StorageError::Busy(_) => ErrorCategory::RateLimited,
        StorageError::NotFound(_) => ErrorCategory::NotFound,
        StorageError::SchemaDrift { .. } => ErrorCategory::SchemaDrift,
        StorageError::InvalidConfiguration(_)
        | StorageError::InvalidDraft(_)
        | StorageError::UnsupportedStorageVersion(_)
        | StorageError::AlreadyExists(_)
        | StorageError::InvalidTimestampOrder(_)
        | StorageError::InvalidManifest(_)
        | StorageError::Snapshot(_) => ErrorCategory::InvalidConfiguration,
        StorageError::Sequence { .. }
        | StorageError::LineageMismatch { .. }
        | StorageError::EnvelopeLimitExceeded { .. }
        | StorageError::PartitionLimitExceeded { .. }
        | StorageError::RowLimitExceeded { .. }
        | StorageError::StoredByteLimitExceeded { .. } => ErrorCategory::InvalidData,
        StorageError::ArithmeticOverflow(_)
        | StorageError::Integrity { .. }
        | StorageError::Io { .. }
        | StorageError::Database(_)
        | StorageError::Parquet(_)
        | StorageError::Serialization(_)
        | StorageError::Batch(_)
        | StorageError::ActivityState => ErrorCategory::Internal,
    }
}

fn storage_retryable(error: &StorageError) -> bool {
    matches!(error, StorageError::Busy(_))
}

pub(crate) fn map_context_error(error: ConnectorError) -> EngineError {
    EngineError::from_connector(error)
}

pub(crate) fn deadline_too_long(_limit: Duration) -> EngineError {
    EngineError::BoundExceeded("request deadline exceeds ENGINE_MAX_DEADLINE")
}

pub(crate) fn live_payload_guard(live: u8) -> Result<(), EngineError> {
    if live > MAX_LIVE_COLUMNAR_PAYLOADS {
        return Err(EngineError::peak_exceeded());
    }
    Ok(())
}

pub(crate) fn peak_guard(bytes: usize) -> Result<(), EngineError> {
    if bytes > MAX_ENGINE_PEAK_BYTES {
        return Err(EngineError::peak_exceeded());
    }
    Ok(())
}
