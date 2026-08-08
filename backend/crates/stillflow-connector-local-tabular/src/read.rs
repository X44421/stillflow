use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io::{BufReader, Cursor};
use std::num::NonZeroUsize;
use std::sync::Arc;

use futures::stream;
use polars::io::mmap::MmapBytesReader;
use polars::prelude::{
    Column, CsvReadOptions, DataFrame, DataType as PolarsDataType, JsonFormat, JsonReader,
    ParallelStrategy, ParquetReader, SerReader,
};
use serde::de::{DeserializeSeed, Error as DeError, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Value};
use stillflow_connectors::RawBatchStream;
use stillflow_core::{
    BatchEnvelope, ColumnId, ConnectorError, ConnectorResult, ErrorCategory, LogicalField,
    LogicalSchema, LogicalType, RequestContext, SourceAsset,
};

use crate::bridge::dataframe_to_record_batch;
use crate::config::LocalTabularConfig;
use crate::format::TabularFormat;
use crate::inspect::{
    inspect_opened_asset, validate_override_against_source, validate_parquet_magic,
    validate_parquet_override_against_source,
};
use crate::json_stream::JsonObjectStream;
use crate::path::RootSet;
use crate::schema::{
    logical_schema_from_polars_arrow, polars_schema_from_logical, project_schema, Projection,
};

const INTERNAL_ROWS: usize = 4_096;

pub(crate) struct PreparedReader {
    context: RequestContext,
    asset_id: uuid::Uuid,
    full_schema: LogicalSchema,
    output_schema: Arc<LogicalSchema>,
    projection: Projection,
    kind: ReaderKind,
    pending: VecDeque<DataFrame>,
    batch_size: usize,
    max_rows: Option<usize>,
    rows_emitted: usize,
    sequence: u64,
    pub(crate) warnings: Vec<String>,
}

pub(crate) struct PrepareOptions<'a> {
    pub(crate) schema_override: Option<&'a LogicalSchema>,
    pub(crate) projection_ids: Option<&'a [ColumnId]>,
    pub(crate) batch_size: usize,
    pub(crate) max_rows: Option<usize>,
    pub(crate) context: &'a RequestContext,
}

enum ReaderKind {
    Empty,
    CountedRows(usize),
    Csv(CsvState),
    Json(JsonObjectStream<BufReader<std::fs::File>>),
    Parquet(ParquetState),
}

struct CsvState {
    decoder: polars::prelude::OwnedBatchedCsvReader,
    validator: csv::Reader<std::fs::File>,
    schema: LogicalSchema,
    row: usize,
}

struct ParquetState {
    file: std::fs::File,
    metadata: polars::io::parquet::metadata::FileMetadataRef,
    projection: Vec<usize>,
    offset: usize,
    finished: bool,
}

pub(crate) fn prepare_reader(
    config: &LocalTabularConfig,
    roots: &RootSet,
    asset: &SourceAsset,
    options: PrepareOptions<'_>,
) -> ConnectorResult<PreparedReader> {
    let PrepareOptions {
        schema_override,
        projection_ids,
        batch_size,
        max_rows,
        context,
    } = options;
    context.ensure_active()?;
    let inspection_opened = roots.open_asset(asset)?;
    let metadata = inspect_opened_asset(inspection_opened, asset, config, context)?;
    let source_schema = metadata.schema.clone();
    let full_schema = if let Some(override_schema) = schema_override {
        validate_override_against_source(override_schema, &source_schema)?;
        if metadata.format == TabularFormat::Parquet.name() {
            validate_parquet_override_against_source(override_schema, &source_schema)?;
        }
        override_schema.clone()
    } else {
        metadata.schema
    };
    let warnings = metadata
        .findings
        .into_iter()
        .map(|finding| finding.code)
        .collect();
    let projection = project_schema(&full_schema, projection_ids)?;
    let output_schema = Arc::new(projection.schema.clone());
    let mut opened = roots.open_asset(asset)?;
    context.ensure_active()?;

    let kind = match opened.format {
        TabularFormat::Csv | TabularFormat::Tsv if full_schema.fields.is_empty() => {
            ReaderKind::Empty
        }
        TabularFormat::Csv | TabularFormat::Tsv => {
            let (separator, quote, has_header) = if opened.format == TabularFormat::Csv {
                (
                    config.csv_delimiter,
                    config.csv_quote,
                    config.csv_has_header,
                )
            } else {
                (b'\t', b'"', config.tsv_has_header)
            };
            let schema = polars_schema_from_logical(&full_schema)?;
            let options = CsvReadOptions::default()
                .with_has_header(has_header)
                .with_schema(Some(Arc::clone(&schema)))
                .with_projection(Some(Arc::new(projection.source_indices.clone())))
                .with_n_rows(max_rows)
                .with_chunk_size(INTERNAL_ROWS.min(batch_size))
                .with_low_memory(true)
                .with_raise_if_empty(false)
                .with_ignore_errors(false)
                .map_parse_options(|options| {
                    options
                        .with_separator(separator)
                        .with_quote_char(Some(quote))
                        .with_try_parse_dates(true)
                        .with_missing_is_null(true)
                        .with_truncate_ragged_lines(false)
                });
            let validation_file = roots.open_asset(asset)?.file;
            let mut validator = csv::ReaderBuilder::new()
                .delimiter(separator)
                .quote(quote)
                .has_headers(has_header)
                .flexible(false)
                .from_reader(validation_file);
            if has_header {
                let headers = validator.headers().map_err(|_| {
                    source_error(
                        ErrorCategory::InvalidData,
                        false,
                        "delimited source has a malformed header",
                    )
                })?;
                let matches_schema = headers.len() == source_schema.fields.len()
                    && headers
                        .iter()
                        .zip(&source_schema.fields)
                        .all(|(name, field)| name == field.name);
                if !matches_schema {
                    return Err(source_error(
                        ErrorCategory::SchemaDrift,
                        false,
                        "delimited source header changed after inspection",
                    ));
                }
            }
            let file: Box<dyn MmapBytesReader> = Box::new(opened.file);
            let decoder = options
                .into_reader_with_file_handle(file)
                .batched(None)
                .map_err(polars_open_error)?;
            ReaderKind::Csv(CsvState {
                decoder,
                validator,
                schema: full_schema.clone(),
                row: 0,
            })
        }
        TabularFormat::Json | TabularFormat::Ndjson => ReaderKind::Json(JsonObjectStream::new(
            BufReader::new(opened.file),
            opened.format,
        )?),
        TabularFormat::Parquet if full_schema.fields.is_empty() => {
            validate_parquet_magic(&mut opened.file, opened.size_bytes)?;
            let mut reader = ParquetReader::new(opened.file);
            let schema = reader.schema().map_err(polars_open_error)?;
            if !schema.is_empty() {
                return Err(source_error(
                    ErrorCategory::SchemaDrift,
                    false,
                    "Parquet schema changed after inspection",
                ));
            }
            let rows = reader.num_rows().map_err(polars_open_error)?;
            ReaderKind::CountedRows(rows)
        }
        TabularFormat::Parquet => {
            validate_parquet_magic(&mut opened.file, opened.size_bytes)?;
            let mut metadata_reader =
                ParquetReader::new(opened.file.try_clone().map_err(|_| {
                    source_error(
                        ErrorCategory::TransientSource,
                        true,
                        "Parquet source handle could not be duplicated",
                    )
                })?);
            let current_schema = metadata_reader.schema().map_err(polars_open_error)?;
            let current_schema =
                logical_schema_from_polars_arrow(asset.id, current_schema.as_ref())?;
            if current_schema != source_schema {
                return Err(source_error(
                    ErrorCategory::SchemaDrift,
                    false,
                    "Parquet schema changed after inspection",
                ));
            }
            let metadata = metadata_reader
                .get_metadata()
                .map_err(polars_open_error)?
                .clone();
            ReaderKind::Parquet(ParquetState {
                file: opened.file,
                metadata,
                projection: projection.source_indices.clone(),
                offset: 0,
                finished: false,
            })
        }
    };

    Ok(PreparedReader {
        context: context.clone(),
        asset_id: asset.id,
        full_schema,
        output_schema,
        projection,
        kind,
        pending: VecDeque::new(),
        batch_size,
        max_rows,
        rows_emitted: 0,
        sequence: 0,
        warnings,
    })
}

impl PreparedReader {
    pub(crate) fn output_schema(&self) -> LogicalSchema {
        self.output_schema.as_ref().clone()
    }

    pub(crate) fn into_raw_stream(self) -> RawBatchStream {
        let stream = stream::try_unfold(self, |mut state| async move {
            match state.next_envelope().await? {
                Some(envelope) => Ok(Some((envelope, state))),
                None => Ok(None),
            }
        });
        RawBatchStream::new(Box::pin(stream))
    }

    pub(crate) async fn next_envelope(&mut self) -> ConnectorResult<Option<BatchEnvelope>> {
        self.context.ensure_active()?;
        let Some(frame) = self.next_output_frame().await? else {
            return Ok(None);
        };
        self.context.ensure_active()?;
        let batch = dataframe_to_record_batch(frame, &self.output_schema)?;
        let envelope = BatchEnvelope::try_new(
            Arc::clone(&self.output_schema),
            self.asset_id,
            self.sequence,
            batch,
        )
        .map_err(|_| {
            source_error(
                ErrorCategory::InvalidData,
                false,
                "decoded batch exceeds the public envelope bounds",
            )
        })?;
        self.sequence = self.sequence.checked_add(1).ok_or_else(|| {
            source_error(
                ErrorCategory::InvalidData,
                false,
                "batch sequence exceeded the supported range",
            )
        })?;
        self.rows_emitted = self
            .rows_emitted
            .checked_add(envelope.row_count())
            .ok_or_else(|| {
                source_error(
                    ErrorCategory::InvalidData,
                    false,
                    "decoded row count exceeded the supported range",
                )
            })?;
        self.context.ensure_active()?;
        Ok(Some(envelope))
    }

    async fn next_output_frame(&mut self) -> ConnectorResult<Option<DataFrame>> {
        let remaining = self
            .max_rows
            .map(|limit| limit.saturating_sub(self.rows_emitted))
            .unwrap_or(usize::MAX);
        if remaining == 0 {
            return Ok(None);
        }
        let target = self.batch_size.min(remaining);
        let mut output: Option<DataFrame> = None;
        let mut output_rows = 0_usize;

        while output_rows < target {
            self.context.ensure_active()?;
            if self.pending.is_empty() {
                self.fill_pending().await?;
            }
            let Some(mut frame) = self.pending.pop_front() else {
                break;
            };
            if frame.height() == 0 {
                continue;
            }
            let take = (target - output_rows).min(frame.height());
            if take < frame.height() {
                let rest = frame.slice(take as i64, frame.height() - take);
                frame = frame.slice(0, take);
                self.pending.push_front(rest);
            }
            output_rows += frame.height();
            if let Some(output) = &mut output {
                output.vstack_mut(&frame).map_err(polars_data_error)?;
            } else {
                output = Some(frame);
            }
        }
        Ok(output)
    }

    async fn fill_pending(&mut self) -> ConnectorResult<()> {
        match &mut self.kind {
            ReaderKind::Empty => {}
            ReaderKind::CountedRows(remaining) => {
                let request_remaining = self
                    .max_rows
                    .map(|limit| limit.saturating_sub(self.rows_emitted))
                    .unwrap_or(usize::MAX);
                let rows = (*remaining)
                    .min(INTERNAL_ROWS)
                    .min(self.batch_size)
                    .min(request_remaining);
                if rows > 0 {
                    *remaining -= rows;
                    self.pending.push_back(empty_frame_with_height(rows)?);
                }
            }
            ReaderKind::Csv(reader) => {
                if let Some(frames) = reader.decoder.next_batches(1).map_err(polars_data_error)? {
                    for frame in frames {
                        reader.validate_rows(frame.height(), &self.context)?;
                        self.pending
                            .push_back(reorder_frame(frame, &self.projection.names)?);
                    }
                }
            }
            ReaderKind::Parquet(reader) => {
                if reader.finished {
                    return Ok(());
                }
                let request_remaining = self
                    .max_rows
                    .map(|limit| limit.saturating_sub(self.rows_emitted))
                    .unwrap_or(usize::MAX);
                let rows = INTERNAL_ROWS.min(self.batch_size).min(request_remaining);
                if rows == 0 {
                    return Ok(());
                }
                let file = reader.file.try_clone().map_err(|_| {
                    source_error(
                        ErrorCategory::TransientSource,
                        true,
                        "Parquet source handle could not be duplicated",
                    )
                })?;
                let mut parquet_reader = ParquetReader::new(file);
                parquet_reader.set_metadata(Arc::clone(&reader.metadata));
                let frame = parquet_reader
                    .with_projection(Some(reader.projection.clone()))
                    .with_slice(Some((reader.offset, rows)))
                    .set_low_memory(true)
                    .read_parallel(ParallelStrategy::None)
                    .finish()
                    .map_err(polars_data_error)?;
                if frame.height() == 0 {
                    reader.finished = true;
                } else {
                    reader.offset = reader.offset.checked_add(frame.height()).ok_or_else(|| {
                        source_error(
                            ErrorCategory::InvalidData,
                            false,
                            "Parquet row offset exceeded the supported range",
                        )
                    })?;
                    self.pending
                        .push_back(reorder_frame(frame, &self.projection.names)?);
                }
            }
            ReaderKind::Json(reader) => {
                let remaining = self
                    .max_rows
                    .map(|limit| limit.saturating_sub(self.rows_emitted))
                    .unwrap_or(usize::MAX);
                let rows = INTERNAL_ROWS.min(remaining).min(self.batch_size);
                if rows == 0 {
                    return Ok(());
                }
                let mut encoded = Vec::new();
                let mut count = 0_usize;
                while count < rows {
                    self.context.ensure_active()?;
                    let Some(raw) = reader.next_raw_object(&self.context)? else {
                        break;
                    };
                    let object = parse_projected_object(
                        &raw,
                        &self.full_schema,
                        &self.projection.names,
                        reader.row_number(),
                    )?;
                    serde_json::to_writer(&mut encoded, &Value::Object(object)).map_err(|_| {
                        source_error(
                            ErrorCategory::Internal,
                            false,
                            "projected JSON row could not be encoded for Polars",
                        )
                    })?;
                    encoded.push(b'\n');
                    count += 1;
                }
                if count > 0 {
                    if self.projection.names.is_empty() {
                        self.pending.push_back(empty_frame_with_height(count)?);
                        return Ok(());
                    }
                    let schema = polars_schema_from_logical(&self.projection.schema)?;
                    let frame = JsonReader::new(Cursor::new(encoded))
                        .with_json_format(JsonFormat::JsonLines)
                        .with_schema(schema)
                        .with_batch_size(NonZeroUsize::new(count).ok_or_else(|| {
                            source_error(
                                ErrorCategory::Internal,
                                false,
                                "JSON batch size invariant failed",
                            )
                        })?)
                        .with_ignore_errors(false)
                        .finish()
                        .map_err(polars_data_error)?;
                    self.pending
                        .push_back(reorder_frame(frame, &self.projection.names)?);
                }
            }
        }
        Ok(())
    }
}

impl CsvState {
    fn validate_rows(&mut self, count: usize, context: &RequestContext) -> ConnectorResult<()> {
        let mut record = csv::StringRecord::new();
        for _ in 0..count {
            context.ensure_active()?;
            let read = self.validator.read_record(&mut record).map_err(|_| {
                source_error(
                    ErrorCategory::InvalidData,
                    false,
                    "delimited source contains a malformed row",
                )
            })?;
            if !read {
                return Err(source_error(
                    ErrorCategory::InvalidData,
                    false,
                    "delimited decoder row counts are inconsistent",
                ));
            }
            self.row = self.row.checked_add(1).ok_or_else(|| {
                source_error(
                    ErrorCategory::InvalidData,
                    false,
                    "delimited row count exceeds the supported range",
                )
            })?;
            if record.len() != self.schema.fields.len() {
                return Err(source_error_with_row(
                    ErrorCategory::InvalidData,
                    "delimited row width does not match the established schema",
                    self.row,
                ));
            }
            for (field, value) in self.schema.fields.iter().zip(&record) {
                if !csv_value_matches(value, field) {
                    return Err(source_error_with_row(
                        ErrorCategory::SchemaDrift,
                        "delimited value does not match the established schema",
                        self.row,
                    ));
                }
            }
        }
        Ok(())
    }
}

fn csv_value_matches(value: &str, field: &LogicalField) -> bool {
    if value.is_empty() {
        return field.nullable || matches!(field.data_type, LogicalType::Null);
    }
    match &field.data_type {
        LogicalType::Null => false,
        LogicalType::Boolean => matches!(value, "true" | "false"),
        LogicalType::Int8 => value.parse::<i8>().is_ok(),
        LogicalType::Int16 => value.parse::<i16>().is_ok(),
        LogicalType::Int32 => value.parse::<i32>().is_ok(),
        LogicalType::Int64 => value.parse::<i64>().is_ok(),
        LogicalType::UInt8 => value.parse::<u8>().is_ok(),
        LogicalType::UInt16 => value.parse::<u16>().is_ok(),
        LogicalType::UInt32 => value.parse::<u32>().is_ok(),
        LogicalType::UInt64 => value.parse::<u64>().is_ok(),
        LogicalType::Float32 => value.parse::<f32>().is_ok_and(|number| number.is_finite()),
        LogicalType::Float64 => value.parse::<f64>().is_ok_and(|number| number.is_finite()),
        LogicalType::Utf8 | LogicalType::Binary => true,
        LogicalType::Date32 | LogicalType::Timestamp { .. } => {
            temporal_text_matches(value, &field.data_type)
        }
        LogicalType::List(_) | LogicalType::Struct(_) => false,
    }
}

fn temporal_text_matches(value: &str, data_type: &LogicalType) -> bool {
    match data_type {
        LogicalType::Date32 => chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").is_ok(),
        LogicalType::Timestamp { timezone, .. } if timezone.is_some() => {
            chrono::DateTime::parse_from_rfc3339(value).is_ok()
        }
        LogicalType::Timestamp { .. } => {
            chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f").is_ok()
                || chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f").is_ok()
        }
        _ => false,
    }
}

fn empty_frame_with_height(height: usize) -> ConnectorResult<DataFrame> {
    let marker = Column::full_null(
        "__stillflow_row_marker".into(),
        height,
        &PolarsDataType::Null,
    );
    DataFrame::new(vec![marker])
        .and_then(|frame| frame.select(Vec::<&str>::new()))
        .map_err(polars_data_error)
}

fn reorder_frame(frame: DataFrame, names: &[String]) -> ConnectorResult<DataFrame> {
    frame
        .select(names.iter().map(String::as_str))
        .map_err(polars_data_error)
}

struct ProjectedObjectSeed<'a> {
    schema: &'a LogicalSchema,
    selected: &'a BTreeSet<&'a str>,
}

impl<'de> DeserializeSeed<'de> for ProjectedObjectSeed<'_> {
    type Value = Map<String, Value>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(ProjectedObjectVisitor {
            schema: self.schema,
            selected: self.selected,
        })
    }
}

struct ProjectedObjectVisitor<'a> {
    schema: &'a LogicalSchema,
    selected: &'a BTreeSet<&'a str>,
}

impl<'de> Visitor<'de> for ProjectedObjectVisitor<'_> {
    type Value = Map<String, Value>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON object matching the established schema")
    }

    fn visit_map<A>(self, mut access: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut output = Map::new();
        let mut seen = BTreeSet::new();
        while let Some(name) = access.next_key::<String>()? {
            let Some(field) = self.schema.fields.iter().find(|field| field.name == name) else {
                return Err(A::Error::custom(
                    "JSON row contains a field outside the established schema",
                ));
            };
            if !seen.insert(name.clone()) {
                return Err(A::Error::custom("JSON row contains a duplicate field"));
            }
            if self.selected.contains(name.as_str()) {
                let value = access.next_value::<Value>()?;
                validate_json_value(&value, field).map_err(A::Error::custom)?;
                output.insert(name, value);
            } else {
                access.next_value_seed(ValidateFieldSeed { field })?;
            }
        }
        for field in &self.schema.fields {
            if !seen.contains(&field.name) && !field.nullable {
                return Err(A::Error::custom("JSON row is missing a required field"));
            }
        }
        Ok(output)
    }
}

struct ValidateFieldSeed<'a> {
    field: &'a LogicalField,
}

impl<'de> DeserializeSeed<'de> for ValidateFieldSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(LogicalValueVisitor {
            data_type: &self.field.data_type,
            nullable: self.field.nullable,
        })
    }
}

struct ValidateTypeSeed<'a> {
    data_type: &'a LogicalType,
}

impl<'de> DeserializeSeed<'de> for ValidateTypeSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(LogicalValueVisitor {
            data_type: self.data_type,
            nullable: true,
        })
    }
}

struct LogicalValueVisitor<'a> {
    data_type: &'a LogicalType,
    nullable: bool,
}

impl LogicalValueVisitor<'_> {
    fn ensure(&self, valid: bool) -> Result<(), &'static str> {
        if valid {
            Ok(())
        } else {
            Err("value has an incompatible logical type")
        }
    }
}

impl<'de> Visitor<'de> for LogicalValueVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a value matching the established logical type")
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        self.ensure(self.nullable || matches!(self.data_type, LogicalType::Null))
            .map_err(E::custom)
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        self.visit_unit()
    }

    fn visit_bool<E>(self, _value: bool) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        self.ensure(matches!(self.data_type, LogicalType::Boolean))
            .map_err(E::custom)
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        let valid = match self.data_type {
            LogicalType::Int8 => i8::try_from(value).is_ok(),
            LogicalType::Int16 => i16::try_from(value).is_ok(),
            LogicalType::Int32 => i32::try_from(value).is_ok(),
            LogicalType::Int64 | LogicalType::Float32 | LogicalType::Float64 => true,
            _ => false,
        };
        self.ensure(valid).map_err(E::custom)
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        let valid = match self.data_type {
            LogicalType::Int8 => i8::try_from(value).is_ok(),
            LogicalType::Int16 => i16::try_from(value).is_ok(),
            LogicalType::Int32 => i32::try_from(value).is_ok(),
            LogicalType::UInt8 => u8::try_from(value).is_ok(),
            LogicalType::UInt16 => u16::try_from(value).is_ok(),
            LogicalType::UInt32 => u32::try_from(value).is_ok(),
            LogicalType::UInt64 | LogicalType::Float32 | LogicalType::Float64 => true,
            LogicalType::Int64 => i64::try_from(value).is_ok(),
            _ => false,
        };
        self.ensure(valid).map_err(E::custom)
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        let valid = match self.data_type {
            LogicalType::Float32 => value.is_finite() && (value as f32).is_finite(),
            LogicalType::Float64 => value.is_finite(),
            _ => false,
        };
        self.ensure(valid).map_err(E::custom)
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        let valid = match self.data_type {
            LogicalType::Utf8 => true,
            LogicalType::Date32 | LogicalType::Timestamp { .. } => {
                temporal_text_matches(value, self.data_type)
            }
            _ => false,
        };
        self.ensure(valid).map_err(E::custom)
    }

    fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        self.visit_str(value)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        self.visit_str(&value)
    }

    fn visit_seq<A>(self, mut access: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let LogicalType::List(element) = self.data_type else {
            return Err(A::Error::custom("array has an incompatible logical type"));
        };
        while access
            .next_element_seed(ValidateTypeSeed { data_type: element })?
            .is_some()
        {}
        Ok(())
    }

    fn visit_map<A>(self, mut access: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let LogicalType::Struct(fields) = self.data_type else {
            return Err(A::Error::custom("object has an incompatible logical type"));
        };
        let mut seen = BTreeSet::new();
        while let Some(name) = access.next_key::<String>()? {
            let Some(field) = fields.iter().find(|field| field.name == name) else {
                return Err(A::Error::custom("nested object contains an unknown field"));
            };
            if !seen.insert(name) {
                return Err(A::Error::custom("nested object contains a duplicate field"));
            }
            access.next_value_seed(ValidateFieldSeed { field })?;
        }
        if fields
            .iter()
            .any(|field| !field.nullable && !seen.contains(&field.name))
        {
            return Err(A::Error::custom(
                "nested object is missing a required field",
            ));
        }
        Ok(())
    }
}

fn parse_projected_object(
    raw: &[u8],
    schema: &LogicalSchema,
    names: &[String],
    row: usize,
) -> ConnectorResult<Map<String, Value>> {
    if raw.iter().copied().find(|byte| !byte.is_ascii_whitespace()) != Some(b'{') {
        return Err(source_error_with_row(
            ErrorCategory::InvalidData,
            "JSON row is not an object",
            row,
        ));
    }
    let selected = names.iter().map(String::as_str).collect::<BTreeSet<_>>();
    let mut deserializer = serde_json::Deserializer::from_slice(raw);
    let mut parsed = ProjectedObjectSeed {
        schema,
        selected: &selected,
    }
    .deserialize(&mut deserializer)
    .map_err(|error| {
        let category = match error.classify() {
            serde_json::error::Category::Syntax | serde_json::error::Category::Eof => {
                ErrorCategory::InvalidData
            }
            serde_json::error::Category::Io => ErrorCategory::TransientSource,
            serde_json::error::Category::Data => ErrorCategory::SchemaDrift,
        };
        source_error_with_row(
            category,
            "JSON row does not match the established schema",
            row,
        )
    })?;
    deserializer.end().map_err(|_| {
        source_error_with_row(ErrorCategory::InvalidData, "JSON row is malformed", row)
    })?;

    let mut ordered = Map::new();
    for name in names {
        if let Some(value) = parsed.remove(name) {
            ordered.insert(name.clone(), value);
        } else {
            ordered.insert(name.clone(), Value::Null);
        }
    }
    Ok(ordered)
}

fn validate_json_value(value: &Value, field: &LogicalField) -> Result<(), &'static str> {
    if value.is_null() {
        return if field.nullable || matches!(field.data_type, LogicalType::Null) {
            Ok(())
        } else {
            Err("required field is null")
        };
    }
    validate_json_type(value, &field.data_type)
}

fn validate_json_type(value: &Value, data_type: &LogicalType) -> Result<(), &'static str> {
    let valid = match data_type {
        LogicalType::Null => false,
        LogicalType::Boolean => value.is_boolean(),
        LogicalType::Int8 => value.as_i64().is_some_and(|v| i8::try_from(v).is_ok()),
        LogicalType::Int16 => value.as_i64().is_some_and(|v| i16::try_from(v).is_ok()),
        LogicalType::Int32 => value.as_i64().is_some_and(|v| i32::try_from(v).is_ok()),
        LogicalType::Int64 => value.as_i64().is_some(),
        LogicalType::UInt8 => value.as_u64().is_some_and(|v| u8::try_from(v).is_ok()),
        LogicalType::UInt16 => value.as_u64().is_some_and(|v| u16::try_from(v).is_ok()),
        LogicalType::UInt32 => value.as_u64().is_some_and(|v| u32::try_from(v).is_ok()),
        LogicalType::UInt64 => value.as_u64().is_some(),
        LogicalType::Float32 => value
            .as_f64()
            .is_some_and(|number| number.is_finite() && (number as f32).is_finite()),
        LogicalType::Float64 => value.as_f64().is_some_and(f64::is_finite),
        LogicalType::Utf8 => value.is_string(),
        LogicalType::Date32 | LogicalType::Timestamp { .. } => value
            .as_str()
            .is_some_and(|value| temporal_text_matches(value, data_type)),
        LogicalType::Binary => false,
        LogicalType::List(element) => value.as_array().is_some_and(|values| {
            values
                .iter()
                .all(|value| value.is_null() || validate_json_type(value, element).is_ok())
        }),
        LogicalType::Struct(fields) => value
            .as_object()
            .is_some_and(|object| validate_json_struct(object, fields).is_ok()),
    };
    if valid {
        Ok(())
    } else {
        Err("value has an incompatible logical type")
    }
}

fn validate_json_struct(
    object: &Map<String, Value>,
    fields: &[LogicalField],
) -> Result<(), &'static str> {
    if object
        .keys()
        .any(|name| !fields.iter().any(|field| field.name == *name))
    {
        return Err("nested object contains an unknown field");
    }
    for field in fields {
        match object.get(&field.name) {
            Some(value) => validate_json_value(value, field)?,
            None if field.nullable => {}
            None => return Err("nested object is missing a required field"),
        }
    }
    Ok(())
}

fn polars_open_error(_error: polars::error::PolarsError) -> ConnectorError {
    source_error(
        ErrorCategory::InvalidData,
        false,
        "source could not be opened by the tabular decoder",
    )
}

fn polars_data_error(error: polars::error::PolarsError) -> ConnectorError {
    let text = error.to_string().to_ascii_lowercase();
    let category = if text.contains("could not parse")
        || text.contains("cannot parse")
        || text.contains("schema")
        || text.contains("dtype")
    {
        ErrorCategory::SchemaDrift
    } else {
        ErrorCategory::InvalidData
    };
    source_error(
        category,
        false,
        "source data is malformed or incompatible with the established schema",
    )
}

fn source_error_with_row(
    category: ErrorCategory,
    message: &'static str,
    row: usize,
) -> ConnectorError {
    ConnectorError::with_category(
        category,
        false,
        format!("{message} at row {row}"),
        Vec::new(),
        BTreeMap::new(),
    )
}

fn source_error(category: ErrorCategory, retryable: bool, message: &'static str) -> ConnectorError {
    ConnectorError::with_category(category, retryable, message, Vec::new(), BTreeMap::new())
}
