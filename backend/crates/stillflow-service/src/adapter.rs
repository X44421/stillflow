//! HTTP mapping helpers: the fixed error/status table, envelope reassembly,
//! and response envelopes. Per the frozen contract §3, the `code`/`message`
//! pair remains the authoritative error contract and the status code is
//! advisory.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{Map, Value};
use uuid::Uuid;

use stillflow_api::{
    ApiError, ApiErrorCode, ApiErrorResponse, ApiRequest, ApiResponse, ApiResult, RequestMetadata,
    API_V1,
};

pub fn status_for(code: ApiErrorCode) -> StatusCode {
    match code {
        ApiErrorCode::UnsupportedVersion | ApiErrorCode::InvalidRequest => StatusCode::BAD_REQUEST,
        ApiErrorCode::NotFound => StatusCode::NOT_FOUND,
        ApiErrorCode::Conflict => StatusCode::CONFLICT,
        ApiErrorCode::LimitExceeded => StatusCode::PAYLOAD_TOO_LARGE,
        ApiErrorCode::Unauthorized => StatusCode::UNAUTHORIZED,
        ApiErrorCode::Internal => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

pub fn error_response(request_id: Uuid, error: ApiError) -> Response {
    let status = status_for(error.code);
    (status, axum::Json(ApiErrorResponse::new(request_id, error))).into_response()
}

pub fn ok_response<T: Serialize>(result: ApiResult<ApiResponse<T>>) -> Response {
    match result {
        Ok(response) => (StatusCode::OK, axum::Json(response)).into_response(),
        Err(error) => error_response(Uuid::nil(), error),
    }
}

fn object_body(body: Value) -> Result<Map<String, Value>, ApiError> {
    match body {
        Value::Object(fields) => Ok(fields),
        _ => Err(ApiError::invalid("request body must be a JSON object")),
    }
}

/// Reassembles a typed `ApiRequest<T>` from the raw JSON envelope, merging
/// manifest path parameters into the body object (contract §3.3).
pub fn parse_body<T: DeserializeOwned>(
    bytes: &[u8],
    path_params: Vec<(String, String)>,
) -> Result<ApiRequest<T>, Response> {
    let raw: ApiRequest<Value> = match serde_json::from_slice(bytes) {
        Ok(raw) => raw,
        Err(_) => {
            return Err(error_response(
                Uuid::nil(),
                ApiError::invalid("request envelope is not valid JSON"),
            ))
        }
    };
    let request_id = raw.meta.request_id;
    let mut fields = match object_body(raw.body) {
        Ok(fields) => fields,
        Err(error) => return Err(error_response(request_id, error)),
    };
    for (key, value) in path_params {
        fields.insert(key, Value::String(value));
    }
    match serde_json::from_value::<T>(Value::Object(fields)) {
        Ok(body) => Ok(ApiRequest {
            meta: raw.meta,
            body,
        }),
        Err(_) => Err(error_response(
            request_id,
            ApiError::invalid("request body does not match the manifest schema"),
        )),
    }
}

pub fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'%' if index + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[index + 1..index + 3]).unwrap_or("");
                match u8::from_str_radix(hex, 16) {
                    Ok(byte) => {
                        out.push(byte);
                        index += 3;
                    }
                    Err(_) => {
                        out.push(b'%');
                        index += 1;
                    }
                }
            }
            b'+' => {
                out.push(b' ');
                index += 1;
            }
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

pub fn parse_query(query: &str) -> Vec<(String, String)> {
    query
        .split('&')
        .filter(|part| !part.is_empty())
        .map(|part| match part.split_once('=') {
            Some((key, value)) => (percent_decode(key), percent_decode(value)),
            None => (percent_decode(part), String::new()),
        })
        .collect()
}

fn loose_value(raw: String) -> Value {
    serde_json::from_str::<Value>(&raw).unwrap_or(Value::String(raw))
}

const META_KEYS: [&str; 4] = ["apiVersion", "requestId", "workspaceId", "idempotencyKey"];

/// Reassembles a typed `ApiRequest<T>` from GET query parameters: envelope
/// meta keys plus top-level body fields (contract §3.3).
pub fn parse_query_envelope<T: DeserializeOwned>(
    query: Option<String>,
    path_params: Vec<(String, String)>,
) -> Result<ApiRequest<T>, Response> {
    let fallback_request_id = Uuid::new_v4();
    let mut meta = Map::new();
    let mut body = Map::new();
    if let Some(query) = query.as_deref() {
        for (key, value) in parse_query(query) {
            if META_KEYS.contains(&key.as_str()) {
                meta.insert(key, loose_value(value));
            } else {
                body.insert(key, loose_value(value));
            }
        }
    }
    for (key, value) in path_params {
        body.insert(key, Value::String(value));
    }
    meta.entry("apiVersion".to_owned())
        .or_insert_with(|| Value::from(API_V1.value()));
    meta.entry("requestId".to_owned())
        .or_insert_with(|| Value::String(fallback_request_id.to_string()));
    meta.entry("workspaceId".to_owned())
        .or_insert_with(|| Value::String(Uuid::nil().to_string()));
    let request_meta: RequestMetadata = match serde_json::from_value(Value::Object(meta)) {
        Ok(meta) => meta,
        Err(_) => {
            return Err(error_response(
                fallback_request_id,
                ApiError::invalid("request metadata does not match the envelope contract"),
            ))
        }
    };
    let body_typed = match serde_json::from_value::<T>(Value::Object(body)) {
        Ok(body) => body,
        Err(_) => {
            return Err(error_response(
                request_meta.request_id,
                ApiError::invalid("request body does not match the manifest schema"),
            ))
        }
    };
    Ok(ApiRequest {
        meta: request_meta,
        body: body_typed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_table_matches_the_frozen_contract() {
        assert_eq!(
            status_for(ApiErrorCode::UnsupportedVersion),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            status_for(ApiErrorCode::InvalidRequest),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(status_for(ApiErrorCode::NotFound), StatusCode::NOT_FOUND);
        assert_eq!(status_for(ApiErrorCode::Conflict), StatusCode::CONFLICT);
        assert_eq!(
            status_for(ApiErrorCode::LimitExceeded),
            StatusCode::PAYLOAD_TOO_LARGE
        );
        assert_eq!(
            status_for(ApiErrorCode::Unauthorized),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            status_for(ApiErrorCode::Internal),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn query_parsing_decodes_and_coerces() {
        let pairs = parse_query("streamKind=job&limit=50&name=a%20b%2Bc");
        assert_eq!(pairs[0], ("streamKind".to_owned(), "job".to_owned()));
        assert_eq!(loose_value(pairs[1].1.clone()), Value::from(50));
        assert_eq!(loose_value(pairs[2].1.clone()), Value::from("a b+c"));
    }

    #[test]
    fn query_envelope_reassembles_meta_and_body() {
        let request = parse_query_envelope::<serde_json::Value>(
            Some("apiVersion=1&requestId=00000000-0000-0000-0000-000000000001&workspaceId=00000000-0000-0000-0000-000000000002&limit=25".to_owned()),
            vec![],
        )
        .expect("query envelope");
        assert_eq!(request.meta.api_version.value(), 1);
        assert_eq!(request.body["limit"], 25);
    }

    #[test]
    fn parse_body_merges_path_params_into_typed_body() {
        let payload = serde_json::json!({
            "meta": {
                "apiVersion": 1,
                "requestId": "00000000-0000-0000-0000-000000000003",
                "workspaceId": "00000000-0000-0000-0000-000000000002"
            },
            "body": {}
        })
        .to_string();
        let request = parse_body::<serde_json::Value>(
            payload.as_bytes(),
            vec![(
                "jobId".to_owned(),
                "00000000-0000-0000-0000-000000000004".to_owned(),
            )],
        )
        .expect("typed body");
        assert_eq!(
            request.body["jobId"],
            "00000000-0000-0000-0000-000000000004"
        );
    }
}
