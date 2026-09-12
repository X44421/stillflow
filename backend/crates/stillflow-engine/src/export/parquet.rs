//! Parquet export writer adapter.

use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::Compression;
use parquet::errors::ParquetError;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use stillflow_core::{
    logical_schema_to_arrow, LogicalSchema, LogicalSchemaFingerprint, MAX_BATCH_ROWS,
};
use stillflow_storage::{StagedExportFile, StorageError};

use super::type_error;
use crate::{EngineError, ENGINE_CONTRACT_VERSION};

pub(super) fn open_parquet_writer<'a>(
    staged: &'a mut StagedExportFile,
    schema: &LogicalSchema,
) -> Result<ArrowWriter<&'a mut StagedExportFile>, EngineError> {
    let arrow_schema = logical_schema_to_arrow(schema)
        .map_err(|_| type_error("export canonical Arrow schema derivation failed"))?;
    let fingerprint = LogicalSchemaFingerprint::try_from_schema(schema)
        .map_err(|_| type_error("export schema fingerprint derivation failed"))?
        .to_string();
    let properties = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_row_count(Some(MAX_BATCH_ROWS))
        .set_key_value_metadata(Some(vec![
            KeyValue::new("stillflow:schema_fingerprint".to_owned(), Some(fingerprint)),
            KeyValue::new(
                "stillflow:export_manifest_version".to_owned(),
                Some(stillflow_core::EXPORT_MANIFEST_VERSION.to_string()),
            ),
            KeyValue::new(
                "stillflow:export_format_contract_version".to_owned(),
                Some(stillflow_core::EXPORT_FORMAT_CONTRACT_VERSION.to_string()),
            ),
            KeyValue::new(
                "stillflow:export_encoder_version".to_owned(),
                Some(stillflow_core::EXPORT_ENCODER_VERSION.to_owned()),
            ),
            KeyValue::new(
                "stillflow:engine_contract_version".to_owned(),
                Some(ENGINE_CONTRACT_VERSION.to_string()),
            ),
        ]))
        .build();
    ArrowWriter::try_new(staged, arrow_schema, Some(properties))
        .map_err(|_| type_error("export Parquet writer initialization failed"))
}

/// Recovers the typed staged-file error from a Parquet sink failure so
/// staging-budget violations stay typed (ADR-004 §5), and maps every other
/// encoder failure onto the internal category.
pub(super) fn map_parquet_error(error: ParquetError) -> EngineError {
    if let ParquetError::External(source) = error {
        if let Ok(io_error) = source.downcast::<std::io::Error>() {
            if let Some(inner) = io_error.into_inner() {
                if let Ok(storage) = inner.downcast::<StorageError>() {
                    return EngineError::from_storage(*storage);
                }
            }
        }
    }
    EngineError::Internal("export Parquet encoding failed")
}
