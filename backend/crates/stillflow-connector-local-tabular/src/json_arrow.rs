//! Bounded `arrow-json` 59 decoder for the E24-B2JSON-A0 experiment.
//!
//! Compiled only under the private `json-arrow-direct` feature. The accepted
//! legacy path remains the default unless `STILLFLOW_JSON_ARROW_DIRECT=1`.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_json::reader::Decoder;
use arrow_json::ReaderBuilder;
use arrow_schema::SchemaRef;
use arrow_select::concat::concat_batches;
use serde_json::{Map, Value};
use stillflow_core::{ConnectorError, ConnectorResult, ErrorCategory};

const DIRECT_SWITCH_ENV: &str = "STILLFLOW_JSON_ARROW_DIRECT";

pub(crate) fn direct_enabled() -> bool {
    match std::env::var(DIRECT_SWITCH_ENV) {
        Ok(value) => {
            value == "1"
                || value.eq_ignore_ascii_case("true")
                || value.eq_ignore_ascii_case("direct")
        }
        Err(_) => false,
    }
}

pub(crate) fn decoder(schema: SchemaRef, batch_size: usize) -> ConnectorResult<Decoder> {
    ReaderBuilder::new(schema)
        .with_batch_size(batch_size.max(1))
        .build_decoder()
        .map_err(decoder_error)
}

pub(crate) fn serialize_object(
    decoder: &mut Decoder,
    object: Map<String, Value>,
) -> ConnectorResult<()> {
    decoder
        .serialize(&[Value::Object(object)])
        .map_err(decoder_error)
}

pub(crate) fn flush(decoder: &mut Decoder) -> ConnectorResult<Option<RecordBatch>> {
    decoder.flush().map_err(decoder_error)
}

pub(crate) fn empty_batch(schema: SchemaRef, rows: usize) -> ConnectorResult<RecordBatch> {
    let options = RecordBatchOptions::new().with_row_count(Some(rows));
    RecordBatch::try_new_with_options(schema, Vec::new(), &options).map_err(|_| {
        source_error(
            ErrorCategory::Internal,
            "the empty Arrow record batch violated a direct-JSON invariant",
        )
    })
}

pub(crate) fn concat(
    schema: SchemaRef,
    left: RecordBatch,
    right: RecordBatch,
) -> ConnectorResult<RecordBatch> {
    concat_batches(&schema, &[left, right]).map_err(decoder_error)
}

pub(crate) fn slice(batch: &RecordBatch, offset: usize, length: usize) -> RecordBatch {
    batch.slice(offset, length)
}

pub(crate) fn align_schema(batch: RecordBatch, schema: SchemaRef) -> ConnectorResult<RecordBatch> {
    if Arc::ptr_eq(&batch.schema(), &schema) {
        return Ok(batch);
    }
    batch.with_schema(schema).map_err(decoder_error)
}

fn decoder_error(error: arrow_schema::ArrowError) -> ConnectorError {
    let text = error.to_string().to_ascii_lowercase();
    let category = if text.contains("schema") || text.contains("dtype") || text.contains("type") {
        ErrorCategory::SchemaDrift
    } else {
        ErrorCategory::InvalidData
    };
    source_error(
        category,
        "source data is malformed or incompatible with the established schema",
    )
}

fn source_error(category: ErrorCategory, message: &'static str) -> ConnectorError {
    ConnectorError::with_category(category, false, message, Vec::new(), BTreeMap::new())
}
