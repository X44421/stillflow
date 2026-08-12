use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom};

use polars::prelude::{ParquetReader, SerReader};
use stillflow_core::{
    AssetMetadata, ConnectorError, ConnectorResult, ErrorCategory, FindingSeverity,
    InspectionFinding, LogicalSchema, RequestContext, SourceAsset,
};

use crate::config::LocalTabularConfig;
use crate::format::TabularFormat;
use crate::inference::infer_text_schema;
use crate::path::OpenedAsset;
use crate::schema::logical_schema_from_polars_arrow;

pub(crate) fn inspect_opened_asset(
    mut opened: OpenedAsset,
    asset: &SourceAsset,
    config: &LocalTabularConfig,
    context: &RequestContext,
) -> ConnectorResult<AssetMetadata> {
    context.ensure_active()?;
    let (schema, row_count, findings) = if opened.format == TabularFormat::Parquet {
        validate_parquet_magic(&mut opened.file, opened.size_bytes)?;
        context.ensure_active()?;
        let mut reader = ParquetReader::new(opened.file);
        let schema = reader.schema().map_err(|_| {
            source_error(
                ErrorCategory::InvalidData,
                false,
                "Parquet footer metadata is malformed",
            )
        })?;
        let logical = logical_schema_from_polars_arrow(asset.id, schema.as_ref())?;
        let rows = reader.num_rows().map_err(|_| {
            source_error(
                ErrorCategory::InvalidData,
                false,
                "Parquet row metadata is malformed",
            )
        })?;
        (logical, Some(rows as u64), Vec::new())
    } else {
        let inference = infer_text_schema(
            opened.file,
            opened.size_bytes,
            opened.format,
            config,
            asset.id,
            context,
        )?;
        let findings = if inference.truncated {
            vec![InspectionFinding {
                code: "inspect.schema_inference_truncated".to_owned(),
                message: "schema inference stopped at the configured row or byte bound".to_owned(),
                severity: FindingSeverity::Warning,
            }]
        } else {
            Vec::new()
        };
        (inference.schema, None, findings)
    };

    context.ensure_active()?;
    Ok(AssetMetadata {
        schema,
        format: opened.format.name().to_owned(),
        size_bytes: Some(opened.size_bytes),
        row_count,
        modified_at: opened.modified_at,
        findings,
    })
}

pub(crate) fn validate_parquet_magic(file: &mut std::fs::File, size: u64) -> ConnectorResult<()> {
    if size < 12 {
        return Err(source_error(
            ErrorCategory::InvalidData,
            false,
            "Parquet source is too short to contain a valid footer",
        ));
    }
    let mut magic = [0_u8; 4];
    file.seek(SeekFrom::Start(0)).map_err(|_| {
        source_error(
            ErrorCategory::TransientSource,
            true,
            "Parquet header could not be read",
        )
    })?;
    file.read_exact(&mut magic).map_err(|_| {
        source_error(
            ErrorCategory::InvalidData,
            false,
            "Parquet header is truncated",
        )
    })?;
    if &magic != b"PAR1" {
        return Err(source_error(
            ErrorCategory::InvalidData,
            false,
            "Parquet header magic is invalid",
        ));
    }
    file.seek(SeekFrom::End(-4)).map_err(|_| {
        source_error(
            ErrorCategory::InvalidData,
            false,
            "Parquet footer could not be located",
        )
    })?;
    file.read_exact(&mut magic).map_err(|_| {
        source_error(
            ErrorCategory::InvalidData,
            false,
            "Parquet footer is truncated",
        )
    })?;
    if &magic != b"PAR1" {
        return Err(source_error(
            ErrorCategory::InvalidData,
            false,
            "Parquet footer magic is invalid",
        ));
    }
    file.seek(SeekFrom::Start(0)).map_err(|_| {
        source_error(
            ErrorCategory::TransientSource,
            true,
            "Parquet source could not be rewound",
        )
    })?;
    Ok(())
}

pub(crate) fn validate_override_against_source(
    override_schema: &LogicalSchema,
    source_schema: &LogicalSchema,
) -> ConnectorResult<()> {
    override_schema.validate().map_err(|_| {
        ConnectorError::invalid_configuration("schema override is not a valid logical schema")
    })?;
    if source_schema.fields.is_empty() {
        return Ok(());
    }
    let source_names = source_schema
        .fields
        .iter()
        .map(|field| field.name.as_str())
        .collect::<Vec<_>>();
    let override_names = override_schema
        .fields
        .iter()
        .map(|field| field.name.as_str())
        .collect::<Vec<_>>();
    if source_names != override_names {
        return Err(source_error(
            ErrorCategory::SchemaDrift,
            false,
            "schema override fields do not match source fields",
        ));
    }
    Ok(())
}

pub(crate) fn validate_parquet_override_against_source(
    override_schema: &LogicalSchema,
    source_schema: &LogicalSchema,
) -> ConnectorResult<()> {
    if override_schema.fields.len() != source_schema.fields.len()
        || source_schema
            .fields
            .iter()
            .zip(&override_schema.fields)
            .any(|(source, target)| !parquet_field_is_compatible(source, target))
    {
        return Err(source_error(
            ErrorCategory::SchemaDrift,
            false,
            "Parquet schema cannot satisfy the requested schema override",
        ));
    }
    Ok(())
}

fn parquet_field_is_compatible(
    source: &stillflow_core::LogicalField,
    target: &stillflow_core::LogicalField,
) -> bool {
    source.name == target.name
        && (!source.nullable || target.nullable)
        && parquet_type_is_compatible(&source.data_type, &target.data_type)
}

fn parquet_type_is_compatible(
    source: &stillflow_core::LogicalType,
    target: &stillflow_core::LogicalType,
) -> bool {
    use stillflow_core::LogicalType;

    match (source, target) {
        (LogicalType::Struct(source), LogicalType::Struct(target)) => {
            source.len() == target.len()
                && source
                    .iter()
                    .zip(target)
                    .all(|(source, target)| parquet_field_is_compatible(source, target))
        }
        (LogicalType::List(source), LogicalType::List(target)) => {
            parquet_type_is_compatible(source, target)
        }
        _ => source
            .least_upper_bound(target)
            .is_ok_and(|joined| joined == *target),
    }
}

fn source_error(category: ErrorCategory, retryable: bool, message: &'static str) -> ConnectorError {
    ConnectorError::with_category(category, retryable, message, Vec::new(), BTreeMap::new())
}
