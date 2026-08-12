use std::collections::{BTreeMap, BTreeSet};

use calamine::{Data, DataType, Range};
use stillflow_core::{
    CellRange, ColumnId, ConnectorError, ConnectorResult, LogicalField, LogicalSchema, LogicalType,
    RequestContext, TimeUnit, WorkbookHeaderSelection, WorkbookRegionSelection,
};
use uuid::Uuid;

use crate::analysis::cell_at;

const MAX_INFERENCE_ROWS: usize = 100_000;
const MAX_HEADER_BYTES: usize = 256;

struct ColumnName {
    display: String,
    original: Option<String>,
}

pub(crate) struct RegionSchema {
    pub(crate) schema: LogicalSchema,
    pub(crate) source_columns: Vec<u32>,
    pub(crate) first_data_row: u32,
    pub(crate) last_data_row: u32,
    pub(crate) data_rows_empty: bool,
}

pub(crate) fn prepare_schema(
    range: &Range<Data>,
    sheet_name: &str,
    asset_id: Uuid,
    selection: WorkbookRegionSelection,
    schema_override: Option<&LogicalSchema>,
    projection: Option<&[ColumnId]>,
    context: &RequestContext,
) -> ConnectorResult<RegionSchema> {
    selection.validate()?;
    let source_columns =
        (selection.range.start.column..=selection.range.end.column).collect::<Vec<_>>();
    let (first_data_row, data_rows_empty) = match selection.header {
        WorkbookHeaderSelection::NoHeader => (selection.range.start.row, false),
        WorkbookHeaderSelection::Row(row) if row == selection.range.end.row => (row, true),
        WorkbookHeaderSelection::Row(row) => (
            row.checked_add(1).ok_or_else(|| {
                ConnectorError::invalid_configuration(
                    "workbook header row exceeds the supported coordinate range",
                )
            })?,
            false,
        ),
    };
    let last_data_row = selection.range.end.row;

    let schema = if let Some(schema) = schema_override {
        schema.validate().map_err(|_| {
            ConnectorError::invalid_configuration("workbook schema override is invalid")
        })?;
        if schema.fields.len() != source_columns.len() {
            return Err(ConnectorError::invalid_configuration(
                "workbook schema override width does not match the selected region",
            ));
        }
        ensure_supported_types(schema)?;
        schema.clone()
    } else {
        infer_schema(
            range,
            sheet_name,
            asset_id,
            selection,
            &source_columns,
            first_data_row,
            last_data_row,
            data_rows_empty,
            context,
        )?
    };
    project_schema(
        schema,
        source_columns,
        projection,
        first_data_row,
        last_data_row,
        data_rows_empty,
    )
}

#[allow(clippy::too_many_arguments)]
fn infer_schema(
    range: &Range<Data>,
    sheet_name: &str,
    asset_id: Uuid,
    selection: WorkbookRegionSelection,
    source_columns: &[u32],
    first_data_row: u32,
    last_data_row: u32,
    data_rows_empty: bool,
    context: &RequestContext,
) -> ConnectorResult<LogicalSchema> {
    let names = column_names(range, selection, source_columns)?;
    let mut observed = vec![
        Observed {
            kind: ObservedKind::Null,
            nullable: data_rows_empty,
        };
        source_columns.len()
    ];
    if !data_rows_empty {
        for (row_offset, row) in (first_data_row..=last_data_row).enumerate() {
            if row_offset >= MAX_INFERENCE_ROWS {
                break;
            }
            if row_offset % 256 == 0 {
                context.ensure_active()?;
            }
            for (index, column) in source_columns.iter().copied().enumerate() {
                let cell = cell_at(range, row, column)?;
                let target = observed.get_mut(index).ok_or_else(|| {
                    ConnectorError::invalid_configuration(
                        "workbook inference column index is invalid",
                    )
                })?;
                target.observe(cell);
            }
        }
    }
    let fields = source_columns
        .iter()
        .copied()
        .zip(names)
        .zip(observed)
        .map(|((column, name), observed)| {
            let mut metadata = BTreeMap::new();
            metadata.insert("stillflow.workbook.sheet".to_owned(), sheet_name.to_owned());
            metadata.insert(
                "stillflow.workbook.sourceColumn".to_owned(),
                column.to_string(),
            );
            metadata.insert(
                "stillflow.workbook.sourceColumnLabel".to_owned(),
                column_label(column),
            );
            metadata.insert(
                "stillflow.workbook.region".to_owned(),
                range_label(selection.range),
            );
            metadata.insert(
                "stillflow.workbook.rangeStart".to_owned(),
                coordinate_label(selection.range.start),
            );
            metadata.insert(
                "stillflow.workbook.rangeEnd".to_owned(),
                coordinate_label(selection.range.end),
            );
            if let WorkbookHeaderSelection::Row(row) = selection.header {
                metadata.insert("stillflow.workbook.headerRow".to_owned(), row.to_string());
            }
            if let Some(original) = name.original {
                metadata.insert("stillflow.workbook.originalHeader".to_owned(), original);
            }
            LogicalField::new(
                stable_column_id(asset_id, column),
                name.display,
                observed.logical_type(),
                observed.nullable,
            )
            .and_then(|field| field.with_metadata(metadata))
            .map_err(|_| {
                ConnectorError::invalid_configuration(
                    "workbook schema could not be represented by logical schema version 1",
                )
            })
        })
        .collect::<ConnectorResult<Vec<_>>>()?;
    LogicalSchema::new(fields).map_err(|_| {
        ConnectorError::invalid_configuration(
            "workbook schema could not be represented by logical schema version 1",
        )
    })
}

fn column_names(
    range: &Range<Data>,
    selection: WorkbookRegionSelection,
    columns: &[u32],
) -> ConnectorResult<Vec<ColumnName>> {
    let mut suffixes = BTreeMap::<String, usize>::new();
    let mut used = BTreeSet::<String>::new();
    columns
        .iter()
        .copied()
        .enumerate()
        .map(|(index, column)| {
            let (base, original) = match selection.header {
                WorkbookHeaderSelection::NoHeader => (format!("column_{}", index + 1), None),
                WorkbookHeaderSelection::Row(row) => {
                    let raw = cell_at(range, row, column)?.to_string();
                    let trimmed = raw.trim();
                    if trimmed.is_empty() {
                        (format!("column_{}", index + 1), Some(raw))
                    } else {
                        (truncate_utf8(trimmed, MAX_HEADER_BYTES), Some(raw))
                    }
                }
            };
            let suffix = suffixes.entry(base.clone()).or_default();
            let display = loop {
                *suffix = suffix.checked_add(1).ok_or_else(|| {
                    ConnectorError::invalid_configuration(
                        "workbook duplicate-header suffix exceeded the supported range",
                    )
                })?;
                let candidate = if *suffix == 1 {
                    base.clone()
                } else {
                    format!("{base}_{}", *suffix)
                };
                if used.insert(candidate.clone()) {
                    break candidate;
                }
            };
            Ok(ColumnName { display, original })
        })
        .collect()
}

fn project_schema(
    schema: LogicalSchema,
    source_columns: Vec<u32>,
    projection: Option<&[ColumnId]>,
    first_data_row: u32,
    last_data_row: u32,
    data_rows_empty: bool,
) -> ConnectorResult<RegionSchema> {
    let Some(ids) = projection else {
        return Ok(RegionSchema {
            schema,
            source_columns,
            first_data_row,
            last_data_row,
            data_rows_empty,
        });
    };
    if ids.is_empty() || ids.iter().copied().collect::<BTreeSet<_>>().len() != ids.len() {
        return Err(ConnectorError::invalid_configuration(
            "workbook projection must contain unique known column ids",
        ));
    }
    let mut fields = Vec::with_capacity(ids.len());
    let mut projected_columns = Vec::with_capacity(ids.len());
    for id in ids {
        let Some((index, field)) = schema
            .fields
            .iter()
            .enumerate()
            .find(|(_, field)| field.id == *id)
        else {
            return Err(ConnectorError::invalid_configuration(
                "workbook projection contains an unknown column id",
            ));
        };
        let column = source_columns.get(index).copied().ok_or_else(|| {
            ConnectorError::invalid_configuration("workbook projection index is invalid")
        })?;
        fields.push(field.clone());
        projected_columns.push(column);
    }
    let schema = LogicalSchema::from_parts(schema.version, fields, schema.metadata.clone())
        .map_err(|_| {
            ConnectorError::invalid_configuration("projected workbook schema is invalid")
        })?;
    Ok(RegionSchema {
        schema,
        source_columns: projected_columns,
        first_data_row,
        last_data_row,
        data_rows_empty,
    })
}

fn ensure_supported_types(schema: &LogicalSchema) -> ConnectorResult<()> {
    for field in &schema.fields {
        if !matches!(
            field.data_type,
            LogicalType::Null
                | LogicalType::Boolean
                | LogicalType::Int64
                | LogicalType::Float64
                | LogicalType::Utf8
                | LogicalType::Timestamp {
                    unit: TimeUnit::Millisecond,
                    timezone: None
                }
        ) {
            return Err(ConnectorError::invalid_configuration(
                "workbook schema override contains an unsupported logical type",
            ));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, Default)]
struct Observed {
    kind: ObservedKind,
    nullable: bool,
}

impl Observed {
    fn observe(&mut self, cell: &Data) {
        let next = match cell {
            Data::Empty => {
                self.nullable = true;
                return;
            }
            Data::Bool(_) => ObservedKind::Boolean,
            Data::Int(_) => ObservedKind::Int64,
            Data::Float(_) => ObservedKind::Float64,
            Data::DateTime(value) if value.is_datetime() => ObservedKind::Timestamp,
            Data::DateTime(_)
            | Data::String(_)
            | Data::DateTimeIso(_)
            | Data::DurationIso(_)
            | Data::Error(_) => ObservedKind::Utf8,
        };
        self.kind = self.kind.join(next);
    }

    const fn logical_type(self) -> LogicalType {
        match self.kind {
            ObservedKind::Null => LogicalType::Null,
            ObservedKind::Boolean => LogicalType::Boolean,
            ObservedKind::Int64 => LogicalType::Int64,
            ObservedKind::Float64 => LogicalType::Float64,
            ObservedKind::Timestamp => LogicalType::Timestamp {
                unit: TimeUnit::Millisecond,
                timezone: None,
            },
            ObservedKind::Utf8 => LogicalType::Utf8,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum ObservedKind {
    #[default]
    Null,
    Boolean,
    Int64,
    Float64,
    Timestamp,
    Utf8,
}

impl ObservedKind {
    const fn join(self, other: Self) -> Self {
        use ObservedKind::{Float64, Int64, Null, Utf8};
        if matches!(self, Null) {
            return other;
        }
        if matches!(other, Null) || self as u8 == other as u8 {
            return self;
        }
        if matches!((self, other), (Int64, Float64) | (Float64, Int64)) {
            return Float64;
        }
        Utf8
    }
}

pub(crate) fn stable_column_id(asset_id: Uuid, source_column: u32) -> ColumnId {
    ColumnId::from_uuid(Uuid::new_v5(
        &asset_id,
        format!("workbook-column:{source_column}").as_bytes(),
    ))
}

fn column_label(mut column: u32) -> String {
    let mut bytes = Vec::new();
    loop {
        let remainder = (column % 26) as u8;
        bytes.push(b'A' + remainder);
        if column < 26 {
            break;
        }
        column = column / 26 - 1;
    }
    bytes.reverse();
    String::from_utf8_lossy(&bytes).into_owned()
}

fn range_label(range: CellRange) -> String {
    format!(
        "{}{}:{}{}",
        column_label(range.start.column),
        u64::from(range.start.row) + 1,
        column_label(range.end.column),
        u64::from(range.end.row) + 1
    )
}

fn coordinate_label(coordinate: stillflow_core::CellCoordinate) -> String {
    format!(
        "{}{}",
        column_label(coordinate.column),
        u64::from(coordinate.row) + 1
    )
}

fn truncate_utf8(value: &str, maximum: usize) -> String {
    if value.len() <= maximum {
        return value.to_owned();
    }
    let mut end = maximum;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value.get(..end).unwrap_or("column").to_owned()
}

#[cfg(test)]
mod tests {
    use calamine::Cell;
    use stillflow_core::{CellCoordinate, WorkbookHeaderSelection};

    use super::*;

    #[test]
    fn infers_and_repairs_duplicate_headers_deterministically() {
        let range = Range::from_sparse(vec![
            Cell::new((0, 0), Data::String("value".into())),
            Cell::new((0, 1), Data::String("value".into())),
            Cell::new((0, 2), Data::String("value_2".into())),
            Cell::new((1, 0), Data::Int(1)),
            Cell::new((1, 1), Data::Float(2.5)),
            Cell::new((1, 2), Data::Bool(true)),
        ]);
        let schema = prepare_schema(
            &range,
            "Sheet1",
            Uuid::from_u128(1),
            WorkbookRegionSelection {
                range: CellRange::try_new(CellCoordinate::new(0, 0), CellCoordinate::new(1, 2))
                    .expect("range"),
                header: WorkbookHeaderSelection::Row(0),
            },
            None,
            None,
            &RequestContext::default(),
        )
        .expect("schema");
        assert_eq!(schema.schema.fields[0].name, "value");
        assert_eq!(schema.schema.fields[1].name, "value_2");
        assert_eq!(schema.schema.fields[2].name, "value_2_2");
        assert_eq!(schema.schema.fields[0].data_type, LogicalType::Int64);
        assert_eq!(schema.schema.fields[1].data_type, LogicalType::Float64);
        assert_eq!(
            schema.schema.fields[0]
                .metadata
                .get("stillflow.workbook.originalHeader")
                .map(String::as_str),
            Some("value")
        );
        assert_eq!(
            schema.schema.fields[0]
                .metadata
                .get("stillflow.workbook.rangeStart")
                .map(String::as_str),
            Some("A1")
        );
    }

    #[test]
    fn infers_mixed_types_nullability_and_projection_order() {
        let range = Range::from_sparse(vec![
            Cell::new((0, 0), Data::Int(1)),
            Cell::new((0, 1), Data::Bool(true)),
            Cell::new((1, 0), Data::Float(2.5)),
            Cell::new((1, 1), Data::Bool(false)),
            Cell::new((2, 1), Data::String("mixed".into())),
        ]);
        let selection = WorkbookRegionSelection {
            range: CellRange::try_new(CellCoordinate::new(0, 0), CellCoordinate::new(2, 1))
                .expect("range"),
            header: WorkbookHeaderSelection::NoHeader,
        };
        let full = prepare_schema(
            &range,
            "Sheet1",
            Uuid::from_u128(2),
            selection,
            None,
            None,
            &RequestContext::default(),
        )
        .expect("schema");
        assert_eq!(full.schema.fields[0].name, "column_1");
        assert_eq!(full.schema.fields[0].data_type, LogicalType::Float64);
        assert!(full.schema.fields[0].nullable);
        assert_eq!(full.schema.fields[1].data_type, LogicalType::Utf8);
        assert!(!full.schema.fields[1].nullable);

        let projection = [full.schema.fields[1].id, full.schema.fields[0].id];
        let projected = prepare_schema(
            &range,
            "Sheet1",
            Uuid::from_u128(2),
            selection,
            None,
            Some(&projection),
            &RequestContext::default(),
        )
        .expect("projected schema");
        assert_eq!(projected.schema.fields[0].name, "column_2");
        assert_eq!(projected.schema.fields[1].name, "column_1");
        assert_eq!(projected.source_columns, vec![1, 0]);
    }

    #[test]
    fn represents_a_header_only_region_as_nullable_null_columns() {
        let range = Range::from_sparse(vec![
            Cell::new((0, 0), Data::String(" name ".into())),
            Cell::new((0, 1), Data::Empty),
        ]);
        let schema = prepare_schema(
            &range,
            "Sheet1",
            Uuid::from_u128(3),
            WorkbookRegionSelection {
                range: CellRange::try_new(CellCoordinate::new(0, 0), CellCoordinate::new(0, 1))
                    .expect("range"),
                header: WorkbookHeaderSelection::Row(0),
            },
            None,
            None,
            &RequestContext::default(),
        )
        .expect("header-only schema");
        assert!(schema.data_rows_empty);
        assert_eq!(schema.schema.fields[0].name, "name");
        assert_eq!(schema.schema.fields[1].name, "column_2");
        assert!(schema
            .schema
            .fields
            .iter()
            .all(|field| field.nullable && field.data_type == LogicalType::Null));
        assert_eq!(
            schema.schema.fields[0]
                .metadata
                .get("stillflow.workbook.originalHeader")
                .map(String::as_str),
            Some(" name ")
        );
    }
}
