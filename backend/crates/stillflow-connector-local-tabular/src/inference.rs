use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufReader, Cursor, Read};

use serde_json::{Map, Value};
use stillflow_core::{
    ConnectorError, ConnectorResult, ErrorCategory, LogicalField, LogicalSchema, LogicalType,
    RequestContext,
};
use uuid::Uuid;

use crate::config::LocalTabularConfig;
use crate::format::TabularFormat;
use crate::json_stream::JsonObjectStream;
use crate::schema::{field_path, stable_column_id};

const READ_CHUNK: usize = 8 * 1024;
const UTF8_BOM: &[u8] = b"\xEF\xBB\xBF";

#[derive(Debug)]
pub(crate) struct InferenceResult {
    pub(crate) schema: LogicalSchema,
    pub(crate) truncated: bool,
}

pub(crate) fn infer_text_schema(
    mut file: File,
    source_size: u64,
    format: TabularFormat,
    config: &LocalTabularConfig,
    asset_id: Uuid,
    context: &RequestContext,
) -> ConnectorResult<InferenceResult> {
    let bytes = read_bounded(&mut file, config.inference_bytes, context)?;
    let byte_truncated = source_size > bytes.len() as u64;
    let bytes = valid_utf8_prefix(bytes, byte_truncated)?;
    match format {
        TabularFormat::Csv | TabularFormat::Tsv => {
            infer_delimited(&bytes, byte_truncated, format, config, asset_id, context)
        }
        TabularFormat::Json | TabularFormat::Ndjson => infer_json(
            &bytes,
            byte_truncated,
            format,
            config.inference_rows,
            asset_id,
            context,
        ),
        TabularFormat::Parquet => Err(ConnectorError::invalid_configuration(
            "Parquet schema is inferred from its footer",
        )),
    }
}

fn read_bounded(
    file: &mut File,
    limit: usize,
    context: &RequestContext,
) -> ConnectorResult<Vec<u8>> {
    let mut output = Vec::with_capacity(limit.min(READ_CHUNK));
    let mut buffer = [0_u8; READ_CHUNK];
    while output.len() < limit {
        context.ensure_active()?;
        let wanted = (limit - output.len()).min(buffer.len());
        let target = buffer.get_mut(..wanted).ok_or_else(|| {
            source_error(
                ErrorCategory::Internal,
                false,
                "schema inference buffer bound is invalid",
            )
        })?;
        let read = file.read(target).map_err(|_| {
            source_error(
                ErrorCategory::TransientSource,
                true,
                "source bytes could not be read during schema inference",
            )
        })?;
        #[cfg(feature = "io-metrics")]
        crate::read::io_metrics::add_inference_phase_bytes(read as u64);
        if read == 0 {
            break;
        }
        let decoded = buffer.get(..read).ok_or_else(|| {
            source_error(
                ErrorCategory::Internal,
                false,
                "schema inference decoder exceeded its input buffer",
            )
        })?;
        output.extend_from_slice(decoded);
    }
    Ok(output)
}

fn valid_utf8_prefix(mut bytes: Vec<u8>, byte_truncated: bool) -> ConnectorResult<Vec<u8>> {
    match std::str::from_utf8(&bytes) {
        Ok(_) => Ok(bytes),
        Err(error) if byte_truncated && error.error_len().is_none() => {
            bytes.truncate(error.valid_up_to());
            Ok(bytes)
        }
        Err(_) => Err(source_error(
            ErrorCategory::InvalidData,
            false,
            "text source is not valid UTF-8",
        )),
    }
}

fn infer_delimited(
    bytes: &[u8],
    byte_truncated: bool,
    format: TabularFormat,
    config: &LocalTabularConfig,
    asset_id: Uuid,
    context: &RequestContext,
) -> ConnectorResult<InferenceResult> {
    let bytes = bytes.strip_prefix(UTF8_BOM).unwrap_or(bytes);
    if bytes.is_empty() {
        return Ok(InferenceResult {
            schema: LogicalSchema::empty(),
            truncated: byte_truncated,
        });
    }
    let (delimiter, quote, has_header) = if format == TabularFormat::Csv {
        (
            config.csv_delimiter,
            config.csv_quote,
            config.csv_has_header,
        )
    } else {
        (b'\t', b'"', config.tsv_has_header)
    };
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .quote(quote)
        .has_headers(has_header)
        .flexible(false)
        .from_reader(Cursor::new(bytes));

    let mut pending_first = None;
    let names = if has_header {
        let headers = reader.headers().map_err(|_| {
            source_error(
                ErrorCategory::InvalidData,
                false,
                "delimited source has a malformed header",
            )
        })?;
        validate_headers(headers)?;
        headers.iter().map(ToOwned::to_owned).collect::<Vec<_>>()
    } else {
        let mut first = csv::StringRecord::new();
        if !reader.read_record(&mut first).map_err(|_| {
            source_error(
                ErrorCategory::InvalidData,
                false,
                "delimited source has a malformed first row",
            )
        })? {
            return Ok(InferenceResult {
                schema: LogicalSchema::empty(),
                truncated: byte_truncated,
            });
        }
        let names = (0..first.len())
            .map(|index| format!("column_{}", index + 1))
            .collect();
        pending_first = Some(first);
        names
    };

    let mut observed = ObservedRecord::new(names);
    let mut sampled = 0_usize;
    let mut row_truncated = false;
    if let Some(first) = pending_first.take() {
        observed.observe_csv(&first)?;
        sampled += 1;
    }
    while sampled < config.inference_rows {
        context.ensure_active()?;
        let mut record = csv::StringRecord::new();
        let read = match reader.read_record(&mut record) {
            Ok(read) => read,
            Err(_) if byte_truncated => {
                row_truncated = true;
                break;
            }
            Err(_) => {
                return Err(source_error(
                    ErrorCategory::InvalidData,
                    false,
                    "delimited source contains a malformed row",
                ));
            }
        };
        if !read {
            break;
        }
        observed.observe_csv(&record)?;
        sampled += 1;
    }
    if sampled == config.inference_rows {
        let offset = usize::try_from(reader.position().byte()).map_err(|_| {
            source_error(
                ErrorCategory::Internal,
                false,
                "delimited parser position exceeds the supported platform range",
            )
        })?;
        row_truncated |= bytes
            .get(offset..)
            .is_some_and(|remaining| remaining.iter().any(|byte| !matches!(byte, b'\r' | b'\n')));
    }

    Ok(InferenceResult {
        schema: observed.into_logical_schema(asset_id)?,
        truncated: byte_truncated || row_truncated,
    })
}

fn validate_headers(headers: &csv::StringRecord) -> ConnectorResult<()> {
    let mut names = BTreeSet::new();
    for name in headers {
        if name.trim().is_empty() {
            return Err(source_error(
                ErrorCategory::InvalidData,
                false,
                "delimited source contains an empty header",
            ));
        }
        if !names.insert(name) {
            return Err(source_error(
                ErrorCategory::InvalidData,
                false,
                "delimited source contains duplicate headers",
            ));
        }
    }
    Ok(())
}

fn infer_json(
    mut bytes: &[u8],
    byte_truncated: bool,
    format: TabularFormat,
    max_rows: usize,
    asset_id: Uuid,
    context: &RequestContext,
) -> ConnectorResult<InferenceResult> {
    if format == TabularFormat::Ndjson && byte_truncated {
        let complete = bytes
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |index| index + 1);
        bytes = bytes.get(..complete).ok_or_else(|| {
            source_error(
                ErrorCategory::Internal,
                false,
                "NDJSON inference boundary exceeded its input buffer",
            )
        })?;
    }
    let reader = BufReader::new(Cursor::new(bytes));
    let mut stream = JsonObjectStream::new(reader, format)?;
    let mut observed = ObservedRecord::default();
    let mut sampled = 0_usize;
    let mut row_truncated = false;
    loop {
        if sampled == max_rows {
            row_truncated = stream.sample_is_truncated()?;
            break;
        }
        match stream.next_object(context) {
            Ok(Some(object)) => {
                observed.observe_json(&object)?;
                sampled += 1;
            }
            Ok(None) => break,
            Err(error) if byte_truncated && is_incomplete_json_error(&error) => {
                row_truncated = true;
                break;
            }
            Err(error) => return Err(error),
        }
    }
    Ok(InferenceResult {
        schema: observed.into_logical_schema(asset_id)?,
        truncated: byte_truncated || row_truncated,
    })
}

fn is_incomplete_json_error(error: &ConnectorError) -> bool {
    matches!(
        error.user_message(),
        "JSON source ended before its top-level array"
            | "JSON array ended before its closing bracket"
            | "JSON object ended before its closing brace"
            | "JSON array ended before its separator or closing bracket"
    )
}

#[derive(Debug, Clone, PartialEq)]
enum ObservedType {
    Null,
    Boolean,
    Int64,
    UInt64,
    Float64,
    Utf8,
    List(Box<ObservedType>),
    Struct(Vec<ObservedField>),
}

#[derive(Debug, Clone, PartialEq)]
struct ObservedField {
    name: String,
    data_type: ObservedType,
    nullable: bool,
}

#[derive(Debug, Default)]
struct ObservedRecord {
    fields: Vec<ObservedField>,
    rows: usize,
}

impl ObservedRecord {
    fn new(names: Vec<String>) -> Self {
        Self {
            fields: names
                .into_iter()
                .map(|name| ObservedField {
                    name,
                    data_type: ObservedType::Null,
                    nullable: false,
                })
                .collect(),
            rows: 0,
        }
    }

    fn observe_csv(&mut self, record: &csv::StringRecord) -> ConnectorResult<()> {
        if record.len() != self.fields.len() {
            return Err(source_error(
                ErrorCategory::InvalidData,
                false,
                "delimited row width does not match its header",
            ));
        }
        for (field, value) in self.fields.iter_mut().zip(record) {
            let observed = observed_csv_scalar(value);
            field.nullable |= matches!(observed, ObservedType::Null);
            field.data_type = merge_type(field.data_type.clone(), observed)?;
        }
        self.rows += 1;
        Ok(())
    }

    fn observe_json(&mut self, object: &Map<String, Value>) -> ConnectorResult<()> {
        let observed = observed_struct(object)?;
        if self.rows == 0 {
            let ObservedType::Struct(fields) = observed else {
                return Err(source_error(
                    ErrorCategory::Internal,
                    false,
                    "JSON inference violated an object-shape invariant",
                ));
            };
            self.fields = fields;
            self.rows = 1;
            return Ok(());
        }
        self.fields = match merge_type(
            ObservedType::Struct(std::mem::take(&mut self.fields)),
            observed,
        )? {
            ObservedType::Struct(fields) => fields,
            _ => {
                return Err(source_error(
                    ErrorCategory::Internal,
                    false,
                    "JSON inference violated a schema-merge invariant",
                ));
            }
        };
        self.rows += 1;
        Ok(())
    }

    fn into_logical_schema(mut self, asset_id: Uuid) -> ConnectorResult<LogicalSchema> {
        if self.rows == 0 {
            for field in &mut self.fields {
                field.nullable = true;
            }
        }
        let fields = materialize_fields(asset_id, "", self.fields)?;
        LogicalSchema::new(fields).map_err(|_| {
            source_error(
                ErrorCategory::InvalidData,
                false,
                "inferred source schema is invalid",
            )
        })
    }
}

fn observed_csv_scalar(value: &str) -> ObservedType {
    if value.is_empty() {
        ObservedType::Null
    } else if matches!(value, "true" | "false") {
        ObservedType::Boolean
    } else if value.parse::<i64>().is_ok() {
        ObservedType::Int64
    } else if value.parse::<u64>().is_ok() {
        ObservedType::UInt64
    } else if value.parse::<f64>().is_ok_and(|number| number.is_finite()) {
        ObservedType::Float64
    } else {
        ObservedType::Utf8
    }
}

fn observed_value(value: &Value) -> ConnectorResult<ObservedType> {
    match value {
        Value::Null => Ok(ObservedType::Null),
        Value::Bool(_) => Ok(ObservedType::Boolean),
        Value::Number(number) if number.as_i64().is_some() => Ok(ObservedType::Int64),
        Value::Number(number) if number.as_u64().is_some() => Ok(ObservedType::UInt64),
        Value::Number(number) if number.as_f64().is_some() => Ok(ObservedType::Float64),
        Value::Number(_) => Err(schema_drift(
            "JSON number is outside supported numeric types",
        )),
        Value::String(_) => Ok(ObservedType::Utf8),
        Value::Array(values) => {
            let mut element = ObservedType::Null;
            for value in values {
                element = merge_type(element, observed_value(value)?)?;
            }
            Ok(ObservedType::List(Box::new(element)))
        }
        Value::Object(object) => observed_struct(object),
    }
}

fn observed_struct(object: &Map<String, Value>) -> ConnectorResult<ObservedType> {
    let fields = object
        .iter()
        .map(|(name, value)| {
            let data_type = observed_value(value)?;
            Ok(ObservedField {
                name: name.clone(),
                nullable: matches!(data_type, ObservedType::Null),
                data_type,
            })
        })
        .collect::<ConnectorResult<Vec<_>>>()?;
    Ok(ObservedType::Struct(fields))
}

fn merge_type(left: ObservedType, right: ObservedType) -> ConnectorResult<ObservedType> {
    use ObservedType::{Boolean, Float64, Int64, List, Null, Struct, UInt64, Utf8};
    if left == right {
        return Ok(left);
    }
    match (left, right) {
        (Null, other) | (other, Null) => Ok(other),
        (Int64 | UInt64 | Float64, Int64 | UInt64 | Float64) => Ok(Float64),
        (List(left), List(right)) => Ok(List(Box::new(merge_type(*left, *right)?))),
        (Struct(left), Struct(right)) => Ok(Struct(merge_struct(left, right)?)),
        (Boolean, Boolean) => Ok(Boolean),
        (Utf8, Utf8) => Ok(Utf8),
        _ => Err(schema_drift(
            "sampled values have incompatible logical types",
        )),
    }
}

fn merge_struct(
    mut left: Vec<ObservedField>,
    mut right: Vec<ObservedField>,
) -> ConnectorResult<Vec<ObservedField>> {
    for field in &mut left {
        if let Some(index) = right
            .iter()
            .position(|candidate| candidate.name == field.name)
        {
            let other = right.remove(index);
            field.data_type = merge_type(field.data_type.clone(), other.data_type)?;
            field.nullable |= other.nullable;
        } else {
            field.nullable = true;
        }
    }
    for mut field in right {
        field.nullable = true;
        left.push(field);
    }
    Ok(left)
}

fn materialize_fields(
    asset_id: Uuid,
    parent: &str,
    fields: Vec<ObservedField>,
) -> ConnectorResult<Vec<LogicalField>> {
    fields
        .into_iter()
        .enumerate()
        .map(|(position, field)| {
            let path = field_path(parent, position, &field.name);
            let data_type = materialize_type(asset_id, &path, field.data_type)?;
            LogicalField::new(
                stable_column_id(asset_id, &path),
                field.name,
                data_type,
                field.nullable,
            )
            .map_err(|_| {
                source_error(
                    ErrorCategory::InvalidData,
                    false,
                    "inferred field is invalid",
                )
            })
        })
        .collect()
}

fn materialize_type(
    asset_id: Uuid,
    parent: &str,
    data_type: ObservedType,
) -> ConnectorResult<LogicalType> {
    Ok(match data_type {
        ObservedType::Null => LogicalType::Null,
        ObservedType::Boolean => LogicalType::Boolean,
        ObservedType::Int64 => LogicalType::Int64,
        ObservedType::UInt64 => LogicalType::UInt64,
        ObservedType::Float64 => LogicalType::Float64,
        ObservedType::Utf8 => LogicalType::Utf8,
        ObservedType::List(element) => {
            LogicalType::List(Box::new(materialize_type(asset_id, parent, *element)?))
        }
        ObservedType::Struct(fields) => {
            LogicalType::Struct(materialize_fields(asset_id, parent, fields)?)
        }
    })
}

fn schema_drift(message: &'static str) -> ConnectorError {
    source_error(ErrorCategory::SchemaDrift, false, message)
}

fn source_error(category: ErrorCategory, retryable: bool, message: &'static str) -> ConnectorError {
    ConnectorError::with_category(category, retryable, message, Vec::new(), BTreeMap::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> LocalTabularConfig {
        LocalTabularConfig {
            allowed_roots: vec!["/".into()],
            max_discovery_depth: 1,
            max_discovered_assets: 10,
            inference_rows: 100,
            inference_bytes: 4096,
            csv_delimiter: b',',
            csv_quote: b'"',
            csv_has_header: true,
            tsv_has_header: true,
            json_direct_projected_writer: false,
        }
    }

    #[test]
    fn infers_nested_json_stably_and_marks_missing_fields_nullable() {
        let bytes = br#"[{"id":1,"nested":{"a":true}},{"id":2,"nested":{}}]"#;
        let result = infer_json(
            bytes,
            false,
            TabularFormat::Json,
            100,
            Uuid::from_u128(7),
            &RequestContext::default(),
        )
        .expect("inference");
        assert_eq!(result.schema.fields.len(), 2);
        let LogicalType::Struct(fields) = &result.schema.fields[1].data_type else {
            panic!("nested struct");
        };
        assert!(fields[0].nullable);
    }

    #[test]
    fn rejects_duplicate_delimited_headers() {
        let result = infer_delimited(
            b"id,id\n1,2\n",
            false,
            TabularFormat::Csv,
            &config(),
            Uuid::from_u128(8),
            &RequestContext::default(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn delimited_nullability_reflects_observed_values() {
        let complete = infer_delimited(
            b"id,label\n1,alpha\n2,beta\n",
            false,
            TabularFormat::Csv,
            &config(),
            Uuid::from_u128(9),
            &RequestContext::default(),
        )
        .expect("complete inference");
        assert!(complete.schema.fields.iter().all(|field| !field.nullable));

        let nullable = infer_delimited(
            b"id,label\n1,\n",
            false,
            TabularFormat::Csv,
            &config(),
            Uuid::from_u128(9),
            &RequestContext::default(),
        )
        .expect("nullable inference");
        assert!(!nullable.schema.fields[0].nullable);
        assert!(nullable.schema.fields[1].nullable);

        let header_only = infer_delimited(
            b"id\n",
            false,
            TabularFormat::Csv,
            &config(),
            Uuid::from_u128(9),
            &RequestContext::default(),
        )
        .expect("header-only inference");
        assert!(header_only.schema.fields[0].nullable);
    }

    #[test]
    fn rejects_incompatible_nested_shapes_during_inference() {
        let error = infer_json(
            br#"[{"nested":{"value":1}},{"nested":{"value":"text"}}]"#,
            false,
            TabularFormat::Json,
            100,
            Uuid::from_u128(10),
            &RequestContext::default(),
        )
        .expect_err("nested type drift");
        assert_eq!(error.category(), ErrorCategory::SchemaDrift);
    }

    #[test]
    fn truncated_json_does_not_hide_malformed_prefixes() {
        for invalid in [
            br#"{"id":1}"#.as_slice(),
            br#"[1,"#.as_slice(),
            br#"[{"id":invalid} "#.as_slice(),
        ] {
            let error = infer_json(
                invalid,
                true,
                TabularFormat::Json,
                100,
                Uuid::from_u128(11),
                &RequestContext::default(),
            )
            .expect_err("malformed prefix");
            assert_eq!(error.category(), ErrorCategory::InvalidData);
        }

        let partial = infer_json(
            br#"[{"id":1"#,
            true,
            TabularFormat::Json,
            100,
            Uuid::from_u128(11),
            &RequestContext::default(),
        )
        .expect("incomplete final object is a bounded sample");
        assert!(partial.truncated);
        assert!(partial.schema.fields.is_empty());
    }

    #[test]
    fn row_bounds_do_not_decode_a_lookahead_row() {
        let exact = infer_json(
            br#"[{"id":1}]"#,
            false,
            TabularFormat::Json,
            1,
            Uuid::from_u128(12),
            &RequestContext::default(),
        )
        .expect("exact JSON sample");
        assert!(!exact.truncated);

        let bounded = infer_json(
            br#"[{"id":1},{"id":"outside-sample"}]"#,
            false,
            TabularFormat::Json,
            1,
            Uuid::from_u128(12),
            &RequestContext::default(),
        )
        .expect("bounded JSON sample");
        assert!(bounded.truncated);
        assert_eq!(bounded.schema.fields[0].data_type, LogicalType::Int64);

        let mut bounded_config = config();
        bounded_config.inference_rows = 1;
        let exact = infer_delimited(
            b"id\n1\n",
            false,
            TabularFormat::Csv,
            &bounded_config,
            Uuid::from_u128(12),
            &RequestContext::default(),
        )
        .expect("exact CSV sample");
        assert!(!exact.truncated);
        let bounded = infer_delimited(
            b"id\n1\noutside-sample\n",
            false,
            TabularFormat::Csv,
            &bounded_config,
            Uuid::from_u128(12),
            &RequestContext::default(),
        )
        .expect("bounded CSV sample");
        assert!(bounded.truncated);
        assert_eq!(bounded.schema.fields[0].data_type, LogicalType::Int64);
    }
}
