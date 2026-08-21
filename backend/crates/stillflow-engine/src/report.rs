//! Report and rejected-row schemas for the E4 experimental slice.

use std::sync::Arc;

use arrow_array::{RecordBatch, StringArray, UInt32Array, UInt64Array};
use stillflow_core::{
    digest_hex, logical_schema_to_arrow, ArtifactProvenanceDraft, BatchEnvelope,
    BatchEnvelopeFactory, ColumnId, LogicalField, LogicalSchema, LogicalType, SourceRowRef,
};
use stillflow_plan::ValidationSeverity;
use uuid::Uuid;

use crate::error::EngineError;

pub const REJECTED_INPUT_KIND_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0011));
pub const REJECTED_INPUT_ID_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0012));
pub const REJECTED_INPUT_VERSION_DIGEST_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0013));
pub const REJECTED_SOURCE_ROW_ORDINAL_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0014));
pub const REJECTED_KIND_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0015));
pub const REJECTED_PLAN_FINGERPRINT_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0016));
pub const REJECTED_CANONICAL_PLAN_DIGEST_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0017));
pub const REJECTED_NODE_ID_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0018));
pub const REJECTED_RULE_ORDINAL_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0019));

const REPORT_COLUMN_BASE: u128 = 0xE4C0_0000_0000_4000_8000_0000_0000_0100;

#[derive(Debug, Clone)]
pub(crate) struct ValidationRuleAccumulator {
    pub node_id: Uuid,
    pub rule_ordinal: u32,
    pub message: String,
    pub evaluated_count: u64,
    pub pass_count: u64,
    pub fail_count: u64,
    pub warning_count: u64,
    pub error_count: u64,
    pub null_count: u64,
    pub false_count: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct ValidationFindingRow {
    pub source: SourceRowRef,
    pub node_id: Uuid,
    pub rule_ordinal: u32,
    pub severity: ValidationSeverity,
    pub predicate_outcome: &'static str,
}

#[derive(Debug, Clone)]
pub(crate) struct DedupRuleAccumulator {
    pub node_id: Uuid,
    pub rule_ordinal: u32,
    pub key_column_count: u32,
    pub evaluated_count: u64,
    pub unique_count: u64,
    pub duplicate_count: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct DuplicateFindingRow {
    pub source: SourceRowRef,
    pub first_source_row_ordinal: u64,
    pub node_id: Uuid,
    pub rule_ordinal: u32,
    pub key_column_count: u32,
    pub encoded_key_byte_count: u32,
}

pub(crate) fn reserved_control_names() -> &'static [&'static str] {
    &[
        "input_kind",
        "input_id",
        "input_version_digest",
        "source_row_ordinal",
        "rejection_kind",
        "plan_fingerprint",
        "canonical_plan_digest",
        "node_id",
        "rule_ordinal",
    ]
}

pub(crate) fn rejected_schema(scan_output: &LogicalSchema) -> Result<LogicalSchema, EngineError> {
    let mut fields = scan_output.fields.clone();
    fields.push(required_utf8(REJECTED_INPUT_KIND_COLUMN_ID, "input_kind")?);
    fields.push(required_utf8(REJECTED_INPUT_ID_COLUMN_ID, "input_id")?);
    fields.push(required_utf8(
        REJECTED_INPUT_VERSION_DIGEST_COLUMN_ID,
        "input_version_digest",
    )?);
    fields.push(required_u64(
        REJECTED_SOURCE_ROW_ORDINAL_COLUMN_ID,
        "source_row_ordinal",
    )?);
    fields.push(required_utf8(REJECTED_KIND_COLUMN_ID, "rejection_kind")?);
    fields.push(required_utf8(
        REJECTED_PLAN_FINGERPRINT_COLUMN_ID,
        "plan_fingerprint",
    )?);
    fields.push(required_utf8(
        REJECTED_CANONICAL_PLAN_DIGEST_COLUMN_ID,
        "canonical_plan_digest",
    )?);
    fields.push(required_utf8(REJECTED_NODE_ID_COLUMN_ID, "node_id")?);
    fields.push(required_u32(
        REJECTED_RULE_ORDINAL_COLUMN_ID,
        "rule_ordinal",
    )?);
    LogicalSchema::new(fields).map_err(|_| EngineError::InvalidPlan("rejected schema is invalid"))
}

pub(crate) fn validation_summary_schema() -> Result<LogicalSchema, EngineError> {
    report_schema(&[
        ("input_kind", LogicalType::Utf8, false),
        ("input_id", LogicalType::Utf8, false),
        ("input_version_digest", LogicalType::Utf8, false),
        ("plan_fingerprint", LogicalType::Utf8, false),
        ("canonical_plan_digest", LogicalType::Utf8, false),
        ("node_id", LogicalType::Utf8, false),
        ("rule_ordinal", LogicalType::UInt32, false),
        ("message", LogicalType::Utf8, false),
        ("evaluated_count", LogicalType::UInt64, false),
        ("pass_count", LogicalType::UInt64, false),
        ("fail_count", LogicalType::UInt64, false),
        ("warning_count", LogicalType::UInt64, false),
        ("error_count", LogicalType::UInt64, false),
        ("null_count", LogicalType::UInt64, false),
        ("false_count", LogicalType::UInt64, false),
    ])
}

pub(crate) fn validation_finding_schema() -> Result<LogicalSchema, EngineError> {
    report_schema(&[
        ("input_kind", LogicalType::Utf8, false),
        ("input_id", LogicalType::Utf8, false),
        ("input_version_digest", LogicalType::Utf8, false),
        ("source_row_ordinal", LogicalType::UInt64, false),
        ("plan_fingerprint", LogicalType::Utf8, false),
        ("canonical_plan_digest", LogicalType::Utf8, false),
        ("node_id", LogicalType::Utf8, false),
        ("rule_ordinal", LogicalType::UInt32, false),
        ("severity", LogicalType::Utf8, false),
        ("predicate_outcome", LogicalType::Utf8, false),
    ])
}

pub(crate) fn dedup_summary_schema() -> Result<LogicalSchema, EngineError> {
    report_schema(&[
        ("input_kind", LogicalType::Utf8, false),
        ("input_id", LogicalType::Utf8, false),
        ("input_version_digest", LogicalType::Utf8, false),
        ("plan_fingerprint", LogicalType::Utf8, false),
        ("canonical_plan_digest", LogicalType::Utf8, false),
        ("node_id", LogicalType::Utf8, false),
        ("rule_ordinal", LogicalType::UInt32, false),
        ("key_column_count", LogicalType::UInt32, false),
        ("evaluated_count", LogicalType::UInt64, false),
        ("unique_count", LogicalType::UInt64, false),
        ("duplicate_count", LogicalType::UInt64, false),
    ])
}

pub(crate) fn duplicate_finding_schema() -> Result<LogicalSchema, EngineError> {
    report_schema(&[
        ("input_kind", LogicalType::Utf8, false),
        ("input_id", LogicalType::Utf8, false),
        ("input_version_digest", LogicalType::Utf8, false),
        ("source_row_ordinal", LogicalType::UInt64, false),
        ("first_source_row_ordinal", LogicalType::UInt64, false),
        ("plan_fingerprint", LogicalType::Utf8, false),
        ("canonical_plan_digest", LogicalType::Utf8, false),
        ("node_id", LogicalType::Utf8, false),
        ("rule_ordinal", LogicalType::UInt32, false),
        ("key_column_count", LogicalType::UInt32, false),
        ("encoded_key_byte_count", LogicalType::UInt32, false),
    ])
}

pub(crate) fn validation_summary_batch(
    provenance: &ArtifactProvenanceDraft,
    rules: &[ValidationRuleAccumulator],
) -> Result<Option<BatchEnvelope>, EngineError> {
    if rules.is_empty() {
        return Ok(None);
    }
    let schema = validation_summary_schema()?;
    let n = rules.len();
    let meta = identity_columns(provenance, n);
    let batch = RecordBatch::try_new(
        logical_schema_to_arrow(&schema)
            .map_err(|_| EngineError::Internal("report arrow schema"))?,
        vec![
            Arc::new(StringArray::from(meta.input_kind)),
            Arc::new(StringArray::from(meta.input_id)),
            Arc::new(StringArray::from(meta.version_digest)),
            Arc::new(StringArray::from(meta.plan_fingerprint)),
            Arc::new(StringArray::from(meta.canonical_plan_digest)),
            Arc::new(StringArray::from(
                rules
                    .iter()
                    .map(|rule| rule.node_id.to_string())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt32Array::from(
                rules
                    .iter()
                    .map(|rule| rule.rule_ordinal)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rules
                    .iter()
                    .map(|rule| rule.message.clone())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rules
                    .iter()
                    .map(|rule| rule.evaluated_count)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rules.iter().map(|rule| rule.pass_count).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rules.iter().map(|rule| rule.fail_count).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rules
                    .iter()
                    .map(|rule| rule.warning_count)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rules
                    .iter()
                    .map(|rule| rule.error_count)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rules.iter().map(|rule| rule.null_count).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rules
                    .iter()
                    .map(|rule| rule.false_count)
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .map_err(|_| EngineError::Internal("validation summary batch"))?;
    envelope(&schema, provenance.input.input.input.id(), batch)
}

pub(crate) fn validation_finding_batch(
    provenance: &ArtifactProvenanceDraft,
    findings: &[ValidationFindingRow],
) -> Result<Option<BatchEnvelope>, EngineError> {
    if findings.is_empty() {
        return Ok(None);
    }
    let schema = validation_finding_schema()?;
    let n = findings.len();
    let meta = identity_columns(provenance, n);
    let batch = RecordBatch::try_new(
        logical_schema_to_arrow(&schema)
            .map_err(|_| EngineError::Internal("report arrow schema"))?,
        vec![
            Arc::new(StringArray::from(meta.input_kind)),
            Arc::new(StringArray::from(meta.input_id)),
            Arc::new(StringArray::from(meta.version_digest)),
            Arc::new(UInt64Array::from(
                findings
                    .iter()
                    .map(|row| row.source.source_row_ordinal)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(meta.plan_fingerprint)),
            Arc::new(StringArray::from(meta.canonical_plan_digest)),
            Arc::new(StringArray::from(
                findings
                    .iter()
                    .map(|row| row.node_id.to_string())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt32Array::from(
                findings
                    .iter()
                    .map(|row| row.rule_ordinal)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                findings
                    .iter()
                    .map(|row| match row.severity {
                        ValidationSeverity::Warning => "warning".to_owned(),
                        ValidationSeverity::Error => "error".to_owned(),
                    })
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                findings
                    .iter()
                    .map(|row| row.predicate_outcome.to_owned())
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .map_err(|_| EngineError::Internal("validation finding batch"))?;
    envelope(&schema, provenance.input.input.input.id(), batch)
}

pub(crate) fn dedup_summary_batch(
    provenance: &ArtifactProvenanceDraft,
    rules: &[DedupRuleAccumulator],
) -> Result<Option<BatchEnvelope>, EngineError> {
    if rules.is_empty() {
        return Ok(None);
    }
    let schema = dedup_summary_schema()?;
    let n = rules.len();
    let meta = identity_columns(provenance, n);
    let batch = RecordBatch::try_new(
        logical_schema_to_arrow(&schema)
            .map_err(|_| EngineError::Internal("report arrow schema"))?,
        vec![
            Arc::new(StringArray::from(meta.input_kind)),
            Arc::new(StringArray::from(meta.input_id)),
            Arc::new(StringArray::from(meta.version_digest)),
            Arc::new(StringArray::from(meta.plan_fingerprint)),
            Arc::new(StringArray::from(meta.canonical_plan_digest)),
            Arc::new(StringArray::from(
                rules
                    .iter()
                    .map(|rule| rule.node_id.to_string())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt32Array::from(
                rules
                    .iter()
                    .map(|rule| rule.rule_ordinal)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt32Array::from(
                rules
                    .iter()
                    .map(|rule| rule.key_column_count)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rules
                    .iter()
                    .map(|rule| rule.evaluated_count)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rules
                    .iter()
                    .map(|rule| rule.unique_count)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rules
                    .iter()
                    .map(|rule| rule.duplicate_count)
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .map_err(|_| EngineError::Internal("dedup summary batch"))?;
    envelope(&schema, provenance.input.input.input.id(), batch)
}

pub(crate) fn duplicate_finding_batch(
    provenance: &ArtifactProvenanceDraft,
    findings: &[DuplicateFindingRow],
) -> Result<Option<BatchEnvelope>, EngineError> {
    if findings.is_empty() {
        return Ok(None);
    }
    let schema = duplicate_finding_schema()?;
    let n = findings.len();
    let meta = identity_columns(provenance, n);
    let batch = RecordBatch::try_new(
        logical_schema_to_arrow(&schema)
            .map_err(|_| EngineError::Internal("report arrow schema"))?,
        vec![
            Arc::new(StringArray::from(meta.input_kind)),
            Arc::new(StringArray::from(meta.input_id)),
            Arc::new(StringArray::from(meta.version_digest)),
            Arc::new(UInt64Array::from(
                findings
                    .iter()
                    .map(|row| row.source.source_row_ordinal)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                findings
                    .iter()
                    .map(|row| row.first_source_row_ordinal)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(meta.plan_fingerprint)),
            Arc::new(StringArray::from(meta.canonical_plan_digest)),
            Arc::new(StringArray::from(
                findings
                    .iter()
                    .map(|row| row.node_id.to_string())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt32Array::from(
                findings
                    .iter()
                    .map(|row| row.rule_ordinal)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt32Array::from(
                findings
                    .iter()
                    .map(|row| row.key_column_count)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt32Array::from(
                findings
                    .iter()
                    .map(|row| row.encoded_key_byte_count)
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .map_err(|_| EngineError::Internal("duplicate finding batch"))?;
    envelope(&schema, provenance.input.input.input.id(), batch)
}

pub(crate) fn rejected_control_arrays(
    provenance: &ArtifactProvenanceDraft,
    ordinals: &[u64],
    kinds: &[String],
    node_ids: &[Uuid],
    rule_ordinals: &[u32],
) -> Result<Vec<Arc<dyn arrow_array::Array>>, EngineError> {
    let n = ordinals.len();
    let meta = identity_columns(provenance, n);
    Ok(vec![
        Arc::new(StringArray::from(meta.input_kind)),
        Arc::new(StringArray::from(meta.input_id)),
        Arc::new(StringArray::from(meta.version_digest)),
        Arc::new(UInt64Array::from(ordinals.to_vec())),
        Arc::new(StringArray::from(kinds.to_vec())),
        Arc::new(StringArray::from(meta.plan_fingerprint)),
        Arc::new(StringArray::from(meta.canonical_plan_digest)),
        Arc::new(StringArray::from(
            node_ids.iter().map(ToString::to_string).collect::<Vec<_>>(),
        )),
        Arc::new(UInt32Array::from(rule_ordinals.to_vec())),
    ])
}

fn report_schema(fields: &[(&str, LogicalType, bool)]) -> Result<LogicalSchema, EngineError> {
    let logical = fields
        .iter()
        .enumerate()
        .map(|(index, (name, data_type, nullable))| {
            LogicalField::new(
                ColumnId::from_uuid(Uuid::from_u128(REPORT_COLUMN_BASE + index as u128)),
                (*name).to_owned(),
                data_type.clone(),
                *nullable,
            )
            .map_err(|_| EngineError::InvalidPlan("report field is invalid"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    LogicalSchema::new(logical).map_err(|_| EngineError::InvalidPlan("report schema is invalid"))
}

fn required_utf8(id: ColumnId, name: &str) -> Result<LogicalField, EngineError> {
    LogicalField::new(id, name.to_owned(), LogicalType::Utf8, false)
        .map_err(|_| EngineError::InvalidPlan("rejected control field is invalid"))
}

fn required_u64(id: ColumnId, name: &str) -> Result<LogicalField, EngineError> {
    LogicalField::new(id, name.to_owned(), LogicalType::UInt64, false)
        .map_err(|_| EngineError::InvalidPlan("rejected control field is invalid"))
}

fn required_u32(id: ColumnId, name: &str) -> Result<LogicalField, EngineError> {
    LogicalField::new(id, name.to_owned(), LogicalType::UInt32, false)
        .map_err(|_| EngineError::InvalidPlan("rejected control field is invalid"))
}

struct IdentityColumns {
    input_kind: Vec<String>,
    input_id: Vec<String>,
    version_digest: Vec<String>,
    plan_fingerprint: Vec<String>,
    canonical_plan_digest: Vec<String>,
}

fn identity_columns(provenance: &ArtifactProvenanceDraft, rows: usize) -> IdentityColumns {
    let input = provenance.input.input;
    IdentityColumns {
        input_kind: vec![input.input.kind_name().to_owned(); rows],
        input_id: vec![input.input.id().to_string(); rows],
        version_digest: vec![digest_hex(&input.version_digest); rows],
        plan_fingerprint: vec![digest_hex(&provenance.plan_fingerprint); rows],
        canonical_plan_digest: vec![digest_hex(&provenance.canonical_plan_digest); rows],
    }
}

fn envelope(
    schema: &LogicalSchema,
    source_asset_id: Uuid,
    batch: RecordBatch,
) -> Result<Option<BatchEnvelope>, EngineError> {
    let factory = BatchEnvelopeFactory::try_new(Arc::new(schema.clone()), source_asset_id)
        .map_err(|_| EngineError::Internal("report envelope factory failed"))?;
    factory
        .try_build(0, batch)
        .map(Some)
        .map_err(|_| EngineError::Internal("report envelope failed"))
}
