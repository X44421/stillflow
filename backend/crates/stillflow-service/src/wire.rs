//! Typed-binary response views over HTTP (contract §6.1, frozen).
//!
//! `asset.preview`, `engine.preview`, and `artifact.content` carry typed
//! binary `Vec<BatchEnvelope>` payloads that are deliberately not JSON
//! serializable. The frozen wire format is an Arrow IPC stream whose schema
//! message carries all StillFlow metadata under one custom-metadata key, so
//! any standard Arrow IPC reader can read the payload while the envelope and
//! view scalars stay lossless. Batches are never JSON-encoded here: this
//! module is the single wire encoding, not a second envelope serialization.

use std::collections::HashMap;
use std::io::Cursor;

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{Schema, SchemaRef};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use stillflow_api::{
    ApiError, ApiResponse, ArtifactContentPage, EnginePreviewView, PreviewView, ResponseMetadata,
};
use stillflow_core::{logical_schema_to_arrow, BatchEnvelope, LogicalSchemaFingerprint};
use stillflow_plan::PlanNodeId;

/// Schema-message custom-metadata key that carries the StillFlow JSON.
pub const WIRE_METADATA_KEY: &str = "stillflow.wire.v1";
/// IANA media type of the Arrow IPC stream format (contract §6.1).
pub const ARROW_STREAM_MEDIA_TYPE: &str = "application/vnd.apache.arrow.stream";
/// Bumped only by a breaking change to the metadata JSON or framing below.
pub const WIRE_VERSION: u32 = 1;

/// StillFlow metadata riding the Arrow IPC schema message (contract §6.1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WireMetadata {
    pub wire_version: u32,
    /// `ApiResponse` envelope metadata, verbatim (§3.1 semantics preserved).
    pub meta: ResponseMetadata,
    /// Shared envelope identity of every batch; `None` for zero-batch views.
    pub envelope: Option<WireEnvelope>,
    /// Index-parallel to the record-batch messages in the stream.
    pub batches: Vec<WireBatchEntry>,
    #[serde(flatten)]
    pub view: WireView,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WireEnvelope {
    pub version: u16,
    pub schema_fingerprint: LogicalSchemaFingerprint,
    pub source_asset_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WireBatchEntry {
    pub sequence: u64,
    pub row_count: usize,
    pub byte_count: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "view", rename_all = "camelCase")]
pub enum WireView {
    #[serde(rename_all = "camelCase")]
    AssetPreview {
        rows_returned: usize,
        bytes_returned: usize,
        rows_truncated: bool,
        bytes_truncated: bool,
        warnings: Vec<String>,
    },
    #[serde(rename_all = "camelCase")]
    EnginePreview {
        plan_fingerprint: String,
        target_node_id: PlanNodeId,
        rows_returned: usize,
        bytes_returned: usize,
        source_rows_scanned: usize,
        source_bytes_scanned: usize,
        rows_truncated: bool,
        bytes_truncated: bool,
        scan_truncated: bool,
        source_exhausted: bool,
    },
    #[serde(rename_all = "camelCase")]
    ArtifactContent {
        next_partition_sequence: Option<u32>,
    },
}

/// Errors while decoding a typed-binary response (contract §6.1 reader rules:
/// every mismatch fails closed).
#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("body is not a valid Arrow IPC stream: {0}")]
    InvalidStream(String),
    #[error("stream schema carries no {key} metadata")]
    MissingMetadata { key: &'static str },
    #[error("stream metadata is not valid {0} JSON: {1}")]
    InvalidMetadata(String, #[source] serde_json::Error),
    #[error("unknown wire version {0}")]
    UnknownWireVersion(u32),
    #[error("batch {index} row count mismatch: metadata declares {declared}, stream has {actual}")]
    RowCountMismatch {
        index: usize,
        declared: usize,
        actual: usize,
    },
    #[error("batch entries and stream batches diverge: metadata declares {declared}, stream has {actual}")]
    BatchCountMismatch { declared: usize, actual: usize },
}

#[derive(Debug)]
pub struct DecodedStream {
    pub metadata: WireMetadata,
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

/// Encodes an `asset.preview` response body (contract §6.1).
pub fn encode_preview_view(response: ApiResponse<PreviewView>) -> Result<Vec<u8>, ApiError> {
    let ApiResponse { meta, body } = response;
    let schema = logical_schema_to_arrow(&body.schema).map_err(|_| internal())?;
    let envelope = shared_envelope(&body.batches)?;
    let batches = batch_entries(&body.batches);
    let metadata = WireMetadata {
        wire_version: WIRE_VERSION,
        meta,
        envelope,
        batches,
        view: WireView::AssetPreview {
            rows_returned: body.rows_returned,
            bytes_returned: body.bytes_returned,
            rows_truncated: body.rows_truncated,
            bytes_truncated: body.bytes_truncated,
            warnings: body.warnings,
        },
    };
    encode_stream(metadata, schema, &owned_payloads(&body.batches))
}

/// Encodes an `engine.preview` response body (contract §6.1).
pub fn encode_engine_preview_view(
    response: ApiResponse<EnginePreviewView>,
) -> Result<Vec<u8>, ApiError> {
    let ApiResponse { meta, body } = response;
    let schema = logical_schema_to_arrow(&body.schema).map_err(|_| internal())?;
    let envelope = shared_envelope(&body.batches)?;
    let batches = batch_entries(&body.batches);
    let metadata = WireMetadata {
        wire_version: WIRE_VERSION,
        meta,
        envelope,
        batches,
        view: WireView::EnginePreview {
            plan_fingerprint: body.plan_fingerprint,
            target_node_id: body.target_node_id,
            rows_returned: body.rows_returned,
            bytes_returned: body.bytes_returned,
            source_rows_scanned: body.source_rows_scanned,
            source_bytes_scanned: body.source_bytes_scanned,
            rows_truncated: body.rows_truncated,
            bytes_truncated: body.bytes_truncated,
            scan_truncated: body.scan_truncated,
            source_exhausted: body.source_exhausted,
        },
    };
    encode_stream(metadata, schema, &owned_payloads(&body.batches))
}

/// Encodes an `artifact.content` response body (contract §6.1). A page with
/// zero batches (cursor past every partition) streams an empty schema.
pub fn encode_artifact_content(
    response: ApiResponse<ArtifactContentPage>,
) -> Result<Vec<u8>, ApiError> {
    let ApiResponse { meta, body } = response;
    let envelopes = &body.batches;
    let schema: SchemaRef = match envelopes.first() {
        Some(first) => first.payload().schema(),
        None => std::sync::Arc::new(Schema::empty()),
    };
    let envelope = shared_envelope(envelopes)?;
    let metadata = WireMetadata {
        wire_version: WIRE_VERSION,
        meta,
        envelope,
        batches: batch_entries(envelopes),
        view: WireView::ArtifactContent {
            next_partition_sequence: body.next_partition_sequence,
        },
    };
    encode_stream(metadata, schema, &owned_payloads(envelopes))
}

/// Decodes a typed-binary response body and applies the §6.1 reader rules
/// (row counts, batch counts, wire version). Clients fail closed on any
/// mismatch.
pub fn decode_stream(bytes: &[u8]) -> Result<DecodedStream, WireError> {
    let mut reader = StreamReader::try_new(Cursor::new(bytes), None)
        .map_err(|error| WireError::InvalidStream(error.to_string()))?;
    let schema = reader.schema();
    let raw = schema
        .metadata()
        .get(WIRE_METADATA_KEY)
        .ok_or(WireError::MissingMetadata {
            key: WIRE_METADATA_KEY,
        })?;
    let metadata: WireMetadata = serde_json::from_str(raw)
        .map_err(|error| WireError::InvalidMetadata(WIRE_METADATA_KEY.to_owned(), error))?;
    if metadata.wire_version != WIRE_VERSION {
        return Err(WireError::UnknownWireVersion(metadata.wire_version));
    }
    let batches: Vec<RecordBatch> = reader
        .by_ref()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| WireError::InvalidStream(error.to_string()))?;
    if metadata.batches.len() != batches.len() {
        return Err(WireError::BatchCountMismatch {
            declared: metadata.batches.len(),
            actual: batches.len(),
        });
    }
    for (index, entry) in metadata.batches.iter().enumerate() {
        let actual = batches[index].num_rows();
        if entry.row_count != actual {
            return Err(WireError::RowCountMismatch {
                index,
                declared: entry.row_count,
                actual,
            });
        }
    }
    Ok(DecodedStream {
        metadata,
        schema,
        batches,
    })
}

fn owned_payloads(envelopes: &[BatchEnvelope]) -> Vec<RecordBatch> {
    envelopes
        .iter()
        .map(|envelope| envelope.payload().clone())
        .collect()
}

fn batch_entries(envelopes: &[BatchEnvelope]) -> Vec<WireBatchEntry> {
    envelopes
        .iter()
        .map(|envelope| WireBatchEntry {
            sequence: envelope.sequence(),
            row_count: envelope.row_count(),
            byte_count: envelope.byte_count(),
        })
        .collect()
}

/// All envelopes of one response view share identity; divergence is a
/// contract violation and fails closed before any bytes are written (§6.1).
fn shared_envelope(envelopes: &[BatchEnvelope]) -> Result<Option<WireEnvelope>, ApiError> {
    let Some(first) = envelopes.first() else {
        return Ok(None);
    };
    let shared = WireEnvelope {
        version: first.version(),
        schema_fingerprint: first.schema_fingerprint(),
        source_asset_id: first.source_asset_id(),
    };
    for envelope in envelopes.iter().skip(1) {
        let diverged = envelope.version() != shared.version
            || envelope.schema_fingerprint() != shared.schema_fingerprint
            || envelope.source_asset_id() != shared.source_asset_id;
        if diverged {
            return Err(ApiError::internal());
        }
    }
    Ok(Some(shared))
}

fn encode_stream(
    metadata: WireMetadata,
    schema: SchemaRef,
    payloads: &[RecordBatch],
) -> Result<Vec<u8>, ApiError> {
    let mut custom = HashMap::with_capacity(1);
    let json = serde_json::to_string(&metadata).map_err(|_| ApiError::internal())?;
    custom.insert(WIRE_METADATA_KEY.to_owned(), json);
    let stream_schema =
        std::sync::Arc::new(Schema::new_with_metadata(schema.fields().clone(), custom));
    let mut writer = StreamWriter::try_new(Vec::new(), stream_schema.as_ref())
        .map_err(|_| ApiError::internal())?;
    for payload in payloads {
        let batch = RecordBatch::try_new(stream_schema.clone(), payload.columns().to_vec())
            .map_err(|_| ApiError::internal())?;
        writer.write(&batch).map_err(|_| ApiError::internal())?;
    }
    writer.finish().map_err(|_| ApiError::internal())?;
    writer.into_inner().map_err(|_| ApiError::internal())
}

fn internal() -> ApiError {
    ApiError::internal()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use stillflow_core::{ColumnId, LogicalField, LogicalSchema, LogicalType};

    fn logical_schema() -> std::sync::Arc<LogicalSchema> {
        std::sync::Arc::new(
            LogicalSchema::new(vec![LogicalField::new(
                ColumnId::random(),
                "value",
                LogicalType::Int64,
                false,
            )
            .expect("logical field")])
            .expect("logical schema"),
        )
    }

    fn envelope_with(
        schema: &std::sync::Arc<LogicalSchema>,
        source_asset_id: Uuid,
        sequence: u64,
        rows: i64,
    ) -> BatchEnvelope {
        let payload = RecordBatch::try_new(
            logical_schema_to_arrow(schema).expect("arrow schema"),
            vec![std::sync::Arc::new(Int64Array::from(vec![rows]))],
        )
        .expect("payload batch");
        BatchEnvelope::try_new(schema.clone(), source_asset_id, sequence, payload)
            .expect("envelope")
    }

    fn sample_metadata(envelopes: &[BatchEnvelope]) -> WireMetadata {
        WireMetadata {
            wire_version: WIRE_VERSION,
            meta: ResponseMetadata {
                api_version: stillflow_api::API_V1,
                request_id: Uuid::new_v4(),
            },
            envelope: shared_envelope(envelopes).expect("uniform"),
            batches: batch_entries(envelopes),
            view: WireView::ArtifactContent {
                next_partition_sequence: Some(7),
            },
        }
    }

    #[test]
    fn stream_round_trips_metadata_and_batches() {
        let schema = logical_schema();
        let asset = Uuid::new_v4();
        let envelopes = vec![
            envelope_with(&schema, asset, 0, 1),
            envelope_with(&schema, asset, 1, 1),
        ];
        let stream_schema = envelopes[0].payload().schema();
        let bytes = encode_stream(
            sample_metadata(&envelopes),
            stream_schema,
            &owned_payloads(&envelopes),
        )
        .expect("encode");
        let decoded = decode_stream(&bytes).expect("decode");
        assert_eq!(decoded.metadata.wire_version, WIRE_VERSION);
        assert_eq!(decoded.batches.len(), 2);
        assert_eq!(
            decoded.metadata.view,
            WireView::ArtifactContent {
                next_partition_sequence: Some(7)
            }
        );
        assert_eq!(
            decoded.metadata.batches[1].sequence,
            envelopes[1].sequence()
        );
    }

    #[test]
    fn view_fields_serialize_camel_case() {
        // Contract §6.1: metadata JSON is camelCase throughout — including the
        // route-specific `view` scalars (serde's enum-level rename_all covers
        // variant names only; each variant renames its own fields).
        let schema = logical_schema();
        let envelopes = vec![envelope_with(&schema, Uuid::new_v4(), 0, 1)];
        let json = serde_json::to_value(sample_metadata(&envelopes)).expect("json");
        assert_eq!(json["view"], "artifactContent");
        assert!(json.get("nextPartitionSequence").is_some());
        assert!(json.get("next_partition_sequence").is_none());
        assert_eq!(json["batches"][0]["rowCount"], 1);
        assert!(json["batches"][0].get("row_count").is_none());
    }

    #[test]
    fn empty_stream_round_trips_without_envelope() {
        let bytes = encode_stream(
            sample_metadata(&[]),
            std::sync::Arc::new(Schema::empty()),
            &[],
        )
        .expect("encode");
        let decoded = decode_stream(&bytes).expect("decode");
        assert!(decoded.batches.is_empty());
        assert!(decoded.metadata.envelope.is_none());
        assert!(decoded.metadata.batches.is_empty());
    }

    #[test]
    fn divergent_envelopes_fail_closed() {
        let schema = logical_schema();
        let envelopes = vec![
            envelope_with(&schema, Uuid::new_v4(), 0, 1),
            envelope_with(&schema, Uuid::new_v4(), 2, 1),
        ];
        let error = shared_envelope(&envelopes).expect_err("divergence is internal");
        assert_eq!(error.code, stillflow_api::ApiErrorCode::Internal);
    }

    #[test]
    fn missing_metadata_fails_closed() {
        let schema = Schema::empty();
        let mut writer =
            StreamWriter::try_new(Vec::new(), &schema).expect("writer without metadata");
        writer.finish().expect("finish");
        let bytes = writer.into_inner().expect("inner");
        let error = decode_stream(&bytes).expect_err("metadata is mandatory");
        assert!(matches!(error, WireError::MissingMetadata { .. }));
    }
}
