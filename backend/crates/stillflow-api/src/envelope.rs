//! Stable request/response envelopes shared by API operations.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{ApiError, ApiErrorCode, ApiResult, ApiVersion, API_V1};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RequestPrincipalKind {
    Member,
    ServiceAccount,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestPrincipal {
    pub kind: RequestPrincipalKind,
    pub id: Uuid,
}

impl RequestPrincipal {
    pub fn member(id: Uuid) -> Self {
        Self {
            kind: RequestPrincipalKind::Member,
            id,
        }
    }

    pub fn service_account(id: Uuid) -> Self {
        Self {
            kind: RequestPrincipalKind::ServiceAccount,
            id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestMetadata {
    pub api_version: ApiVersion,
    pub request_id: Uuid,
    pub workspace_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal: Option<RequestPrincipal>,
}

impl RequestMetadata {
    pub fn new(request_id: Uuid, workspace_id: Uuid) -> Self {
        Self {
            api_version: API_V1,
            request_id,
            workspace_id,
            idempotency_key: None,
            principal: None,
        }
    }

    pub fn with_principal(mut self, principal: RequestPrincipal) -> Self {
        self.principal = Some(principal);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiRequest<T> {
    pub meta: RequestMetadata,
    pub body: T,
}

impl<T> ApiRequest<T> {
    pub fn validate_version(&self) -> ApiResult<()> {
        if self.meta.api_version.is_supported() {
            Ok(())
        } else {
            Err(ApiError::unsupported_version(self.meta.api_version.value()))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResponseMetadata {
    pub api_version: ApiVersion,
    pub request_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiResponse<T> {
    pub meta: ResponseMetadata,
    pub body: T,
}

impl<T> ApiResponse<T> {
    pub fn new(request_id: Uuid, body: T) -> Self {
        Self {
            meta: ResponseMetadata {
                api_version: API_V1,
                request_id,
            },
            body,
        }
    }
}

/// Stable serialized error envelope. Transport adapters may map this to a
/// status code, but the error code/message contract remains transport-neutral.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiErrorResponse {
    pub meta: ResponseMetadata,
    pub error: ApiErrorBody,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiErrorBody {
    pub code: ApiErrorCode,
    pub message: String,
}

impl ApiErrorResponse {
    pub fn new(request_id: Uuid, error: ApiError) -> Self {
        Self {
            meta: ResponseMetadata {
                api_version: API_V1,
                request_id,
            },
            error: ApiErrorBody {
                code: error.code,
                message: error.message,
            },
        }
    }
}
