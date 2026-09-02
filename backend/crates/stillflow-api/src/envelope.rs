//! Stable request/response envelopes shared by API operations.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{ApiError, ApiResult, ApiVersion, API_V1};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestMetadata {
    pub api_version: ApiVersion,
    pub request_id: Uuid,
    pub workspace_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
}

impl RequestMetadata {
    pub fn new(request_id: Uuid, workspace_id: Uuid) -> Self {
        Self {
            api_version: API_V1,
            request_id,
            workspace_id,
            idempotency_key: None,
        }
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
