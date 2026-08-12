use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, Float64Builder, Int64Builder, StringBuilder, TimestampMillisecondBuilder,
};
use arrow_array::{ArrayRef, NullArray, RecordBatch, RecordBatchOptions};
use calamine::{Data, DataType, Range};
use futures::stream;
use stillflow_connectors::RawBatchStream;
use stillflow_core::{
    BatchEnvelope, BatchEnvelopeFactory, ColumnId, ConnectorError, ConnectorResult, ErrorCategory,
    LogicalField, LogicalSchema, LogicalType, RequestContext, SourceAsset, MAX_BATCH_BYTES,
};

use crate::analysis::{cell_at, enforce_sheet_bound, ensure_selection_inside_sheet};
use crate::config::WorkbookConfig;
use crate::path::RootSet;
use crate::preflight::preflight;
use crate::schema::{prepare_schema, RegionSchema};
use crate::workbook::WorkbookReader;

const MAX_CELL_TEXT_BYTES: usize = 8 * 1024 * 1024;
const TARGET_BATCH_BYTES: usize = MAX_BATCH_BYTES / 2;

pub(crate) struct PrepareOptions<'a> {
    pub(crate) schema_override: Option<&'a LogicalSchema>,
    pub(crate) projection_ids: Option<&'a [ColumnId]>,
    pub(crate) batch_size: usize,
    pub(crate) max_rows: Option<usize>,
    pub(crate) context: &'a RequestContext,
}

pub(crate) struct PreparedReader {
    context: RequestContext,
    range: Range<Data>,
    envelope_factory: BatchEnvelopeFactory,
    source_columns: Vec<u32>,
    current_row: u32,
    last_row: u32,
    finished: bool,
    batch_size: usize,
    max_rows: Option<usize>,
    rows_emitted: usize,
    sequence: u64,
    pub(crate) warnings: Vec<String>,
}

pub(crate) fn prepare_reader(
    config: &WorkbookConfig,
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
    let selection = asset.locator.workbook_region.ok_or_else(|| {
        ConnectorError::invalid_configuration(
            "workbook preview and read require an explicit region and header selection",
        )
    })?;
    selection.validate()?;
    let opened = roots.open_asset(asset)?;
    preflight(&opened.file, opened.format, config, context)?;
    let mut workbook = WorkbookReader::open(opened.file, opened.format)?;
    let sheet_name = asset.locator.sheet.as_deref().ok_or_else(|| {
        ConnectorError::invalid_configuration("workbook asset is missing its sheet name")
    })?;
    let sheet = workbook.load_sheet(sheet_name)?;
    enforce_sheet_bound(&sheet.range, config.max_sheet_cells)?;
    ensure_selection_inside_sheet(&sheet.range, selection.range)?;
    let warnings = warnings_for_sheet(&sheet);
    let RegionSchema {
        schema,
        source_columns,
        first_data_row,
        last_data_row,
        data_rows_empty,
    } = prepare_schema(
        &sheet.range,
        sheet_name,
        asset.id,
        selection,
        schema_override,
        projection_ids,
        context,
    )?;
    let envelope_factory = BatchEnvelopeFactory::try_new(Arc::new(schema), asset.id)
        .map_err(|_| invalid_data("workbook schema cannot establish the public batch boundary"))?;
    context.ensure_active()?;
    Ok(PreparedReader {
        context: context.clone(),
        range: sheet.range,
        envelope_factory,
        source_columns,
        current_row: first_data_row,
        last_row: last_data_row,
        finished: data_rows_empty,
        batch_size,
        max_rows,
        rows_emitted: 0,
        sequence: 0,
        warnings,
    })
}

impl PreparedReader {
    pub(crate) fn output_schema(&self) -> LogicalSchema {
        self.envelope_factory.schema().clone()
    }

    pub(crate) fn into_raw_stream(self) -> RawBatchStream {
        let stream = stream::try_unfold(self, |mut state| async move {
            match state.next_envelope()? {
                Some(envelope) => Ok(Some((envelope, state))),
                None => Ok(None),
            }
        });
        RawBatchStream::new(Box::pin(stream))
    }

    pub(crate) fn next_envelope(&mut self) -> ConnectorResult<Option<BatchEnvelope>> {
        self.context.ensure_active()?;
        let Some((start, end)) = self.next_row_window()? else {
            return Ok(None);
        };
        let batch = build_batch(
            &self.range,
            self.envelope_factory.schema(),
            self.envelope_factory.arrow_schema(),
            &self.source_columns,
            start,
            end,
            &self.context,
        )?;
        let envelope = self
            .envelope_factory
            .try_build(self.sequence, batch)
            .map_err(|_| invalid_data("decoded workbook batch exceeds the public bounds"))?;
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or_else(|| invalid_data("workbook batch sequence exceeded the supported range"))?;
        self.rows_emitted = self
            .rows_emitted
            .checked_add(envelope.row_count())
            .ok_or_else(|| invalid_data("workbook row count exceeded the supported range"))?;
        if end == self.last_row {
            self.finished = true;
        } else {
            self.current_row = end.checked_add(1).ok_or_else(|| {
                invalid_data("workbook row coordinate exceeded the supported range")
            })?;
        }
        self.context.ensure_active()?;
        Ok(Some(envelope))
    }

    fn next_row_window(&self) -> ConnectorResult<Option<(u32, u32)>> {
        if self.finished || self.current_row > self.last_row {
            return Ok(None);
        }
        let remaining_limit = self
            .max_rows
            .map(|limit| limit.saturating_sub(self.rows_emitted))
            .unwrap_or(usize::MAX);
        if remaining_limit == 0 {
            return Ok(None);
        }
        let maximum_rows = self.batch_size.min(remaining_limit);
        let mut end = self.current_row;
        let mut rows = 0_usize;
        let mut bytes = 0_usize;
        while end <= self.last_row && rows < maximum_rows {
            let row_bytes = estimate_row(
                &self.range,
                self.envelope_factory.schema(),
                &self.source_columns,
                end,
            )?;
            if row_bytes > TARGET_BATCH_BYTES {
                return Err(invalid_data(
                    "one workbook row exceeds the supported batch byte bound",
                ));
            }
            if rows > 0 && bytes.saturating_add(row_bytes) > TARGET_BATCH_BYTES {
                break;
            }
            bytes = bytes
                .checked_add(row_bytes)
                .ok_or_else(|| invalid_data("workbook batch byte estimate overflow"))?;
            rows += 1;
            if end == self.last_row {
                break;
            }
            end = end.checked_add(1).ok_or_else(|| {
                invalid_data("workbook row coordinate exceeded the supported range")
            })?;
        }
        Ok(Some((self.current_row, end)))
    }
}

fn build_batch(
    range: &Range<Data>,
    schema: &LogicalSchema,
    arrow_schema: &arrow_schema::SchemaRef,
    source_columns: &[u32],
    start_row: u32,
    end_row: u32,
    context: &RequestContext,
) -> ConnectorResult<RecordBatch> {
    let rows_u32 = end_row
        .checked_sub(start_row)
        .and_then(|value| value.checked_add(1))
        .ok_or_else(|| invalid_data("workbook batch row range is invalid"))?;
    let rows = usize::try_from(rows_u32)
        .map_err(|_| invalid_data("workbook batch row count exceeds the platform range"))?;
    if schema.fields.is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(rows));
        return RecordBatch::try_new_with_options(Arc::clone(arrow_schema), Vec::new(), &options)
            .map_err(|_| invalid_data("empty workbook batch violated an Arrow invariant"));
    }
    if schema.fields.len() != source_columns.len() {
        return Err(invalid_data(
            "workbook schema and projected columns are inconsistent",
        ));
    }
    let mut builders = schema
        .fields
        .iter()
        .map(|field| ColumnBuilder::new(field, rows))
        .collect::<ConnectorResult<Vec<_>>>()?;
    for (offset, row) in (start_row..=end_row).enumerate() {
        if offset % 256 == 0 {
            context.ensure_active()?;
        }
        for ((builder, field), column) in builders
            .iter_mut()
            .zip(&schema.fields)
            .zip(source_columns.iter().copied())
        {
            builder.append(cell_at(range, row, column)?, field)?;
        }
    }
    let arrays = builders
        .into_iter()
        .map(ColumnBuilder::finish)
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::clone(arrow_schema), arrays)
        .map_err(|_| invalid_data("workbook arrays do not match the established schema"))
}

enum ColumnBuilder {
    Null { rows: usize },
    Boolean(BooleanBuilder),
    Int64(Int64Builder),
    Float64(Float64Builder),
    Utf8(StringBuilder),
    Timestamp(TimestampMillisecondBuilder),
}

impl ColumnBuilder {
    fn new(field: &LogicalField, capacity: usize) -> ConnectorResult<Self> {
        match field.data_type {
            LogicalType::Null => Ok(Self::Null { rows: 0 }),
            LogicalType::Boolean => Ok(Self::Boolean(BooleanBuilder::with_capacity(capacity))),
            LogicalType::Int64 => Ok(Self::Int64(Int64Builder::with_capacity(capacity))),
            LogicalType::Float64 => Ok(Self::Float64(Float64Builder::with_capacity(capacity))),
            LogicalType::Utf8 => Ok(Self::Utf8(StringBuilder::with_capacity(capacity, 0))),
            LogicalType::Timestamp {
                unit: stillflow_core::TimeUnit::Millisecond,
                timezone: None,
            } => Ok(Self::Timestamp(TimestampMillisecondBuilder::with_capacity(
                capacity,
            ))),
            _ => Err(ConnectorError::invalid_configuration(
                "workbook output schema contains an unsupported logical type",
            )),
        }
    }

    fn append(&mut self, cell: &Data, field: &LogicalField) -> ConnectorResult<()> {
        if cell.is_empty() {
            if !field.nullable && !matches!(field.data_type, LogicalType::Null) {
                return Err(schema_drift("workbook nullability changed after inference"));
            }
            match self {
                Self::Null { rows } => *rows = rows.saturating_add(1),
                Self::Boolean(builder) => builder.append_null(),
                Self::Int64(builder) => builder.append_null(),
                Self::Float64(builder) => builder.append_null(),
                Self::Utf8(builder) => builder.append_null(),
                Self::Timestamp(builder) => builder.append_null(),
            }
            return Ok(());
        }
        match self {
            Self::Null { .. } => Err(schema_drift(
                "workbook value appeared in an established null column",
            )),
            Self::Boolean(builder) => match cell {
                Data::Bool(value) => {
                    builder.append_value(*value);
                    Ok(())
                }
                _ => Err(schema_drift("workbook boolean column changed type")),
            },
            Self::Int64(builder) => match cell {
                Data::Int(value) => {
                    builder.append_value(*value);
                    Ok(())
                }
                _ => Err(schema_drift("workbook integer column changed type")),
            },
            Self::Float64(builder) => match cell {
                Data::Int(value) => {
                    builder.append_value(*value as f64);
                    Ok(())
                }
                Data::Float(value) if value.is_finite() => {
                    builder.append_value(*value);
                    Ok(())
                }
                _ => Err(schema_drift("workbook floating-point column changed type")),
            },
            Self::Utf8(builder) => {
                let value = cell_text(cell)?;
                if value.len() > MAX_CELL_TEXT_BYTES {
                    return Err(invalid_data(
                        "workbook cell text exceeds the supported bound",
                    ));
                }
                builder.append_value(value.as_str());
                Ok(())
            }
            Self::Timestamp(builder) => match cell {
                Data::DateTime(value) if value.is_datetime() => {
                    let value = value.as_datetime().ok_or_else(|| {
                        schema_drift("workbook timestamp is outside the supported range")
                    })?;
                    builder.append_value(value.and_utc().timestamp_millis());
                    Ok(())
                }
                _ => Err(schema_drift("workbook timestamp column changed type")),
            },
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Null { rows } => Arc::new(NullArray::new(rows)),
            Self::Boolean(mut builder) => Arc::new(builder.finish()),
            Self::Int64(mut builder) => Arc::new(builder.finish()),
            Self::Float64(mut builder) => Arc::new(builder.finish()),
            Self::Utf8(mut builder) => Arc::new(builder.finish()),
            Self::Timestamp(mut builder) => Arc::new(builder.finish()),
        }
    }
}

fn estimate_row(
    range: &Range<Data>,
    schema: &LogicalSchema,
    source_columns: &[u32],
    row: u32,
) -> ConnectorResult<usize> {
    schema
        .fields
        .iter()
        .zip(source_columns.iter().copied())
        .try_fold(0_usize, |total, (field, column)| {
            let cell = cell_at(range, row, column)?;
            let value = match field.data_type {
                LogicalType::Utf8 if !cell.is_empty() => cell_text(cell)?.len(),
                LogicalType::Null => 1,
                LogicalType::Boolean => 2,
                LogicalType::Int64 | LogicalType::Float64 | LogicalType::Timestamp { .. } => 9,
                _ => 16,
            };
            total
                .checked_add(value.saturating_add(8))
                .ok_or_else(|| invalid_data("workbook row byte estimate overflow"))
        })
}

fn cell_text(cell: &Data) -> ConnectorResult<String> {
    let value = match cell {
        Data::Empty => String::new(),
        Data::Int(value) => value.to_string(),
        Data::Float(value) if value.is_finite() => value.to_string(),
        Data::Float(_) => return Err(schema_drift("workbook float is not finite")),
        Data::String(value) | Data::DateTimeIso(value) | Data::DurationIso(value) => value.clone(),
        Data::Bool(value) => value.to_string(),
        Data::DateTime(value) if value.is_datetime() => value
            .as_datetime()
            .map(|value| value.format("%Y-%m-%dT%H:%M:%S%.3f").to_string())
            .ok_or_else(|| schema_drift("workbook timestamp is outside the supported range"))?,
        Data::DateTime(value) => value.as_f64().to_string(),
        Data::Error(value) => value.to_string(),
    };
    Ok(value)
}

fn warnings_for_sheet(sheet: &crate::workbook::LoadedSheet) -> Vec<String> {
    let mut warnings = Vec::new();
    if sheet.formulas.used_cells().next().is_some() {
        warnings.push("workbook.formula_cached_values".to_owned());
    }
    if !sheet.merge_metadata_available {
        warnings.push("workbook.merge_metadata_unavailable".to_owned());
    }
    if !sheet.merged.is_empty() {
        warnings.push("workbook.merged_cells".to_owned());
    }
    if !matches!(
        sheet.visibility,
        stillflow_core::WorkbookSheetVisibility::Visible
    ) {
        warnings.push("workbook.hidden_sheet".to_owned());
    }
    warnings.push("workbook.hidden_metadata_unavailable".to_owned());
    warnings
}

fn invalid_data(message: &'static str) -> ConnectorError {
    source_error(ErrorCategory::InvalidData, false, message)
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

    #[test]
    fn reports_sanitized_schema_drift_without_cell_values() {
        let field = LogicalField::new(
            ColumnId::from_uuid(uuid::Uuid::from_u128(1)),
            "value",
            LogicalType::Int64,
            false,
        )
        .expect("field");
        let mut builder = ColumnBuilder::new(&field, 1).expect("builder");
        let error = builder
            .append(&Data::String("sensitive-cell-value".to_owned()), &field)
            .expect_err("mixed value must drift");
        assert_eq!(error.category(), ErrorCategory::SchemaDrift);
        assert!(!error.user_message().contains("sensitive-cell-value"));
    }
}
