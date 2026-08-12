use stillflow_core::{
    AssetMetadata, ConnectorResult, FindingSeverity, InspectionFinding, LogicalSchema,
    RequestContext, SourceAsset, WorkbookHeaderSelection, WorkbookSheetVisibility,
};

use crate::analysis::{analyze_sheet, ensure_selection_inside_sheet};
use crate::config::WorkbookConfig;
use crate::path::OpenedWorkbook;
use crate::preflight::preflight;
use crate::schema::prepare_schema;
use crate::workbook::WorkbookReader;

pub(crate) fn inspect_opened(
    opened: OpenedWorkbook,
    asset: &SourceAsset,
    config: &WorkbookConfig,
    context: &RequestContext,
) -> ConnectorResult<AssetMetadata> {
    context.ensure_active()?;
    let package = preflight(&opened.file, opened.format, config, context)?;
    let format = opened.format;
    let mut workbook = WorkbookReader::open(opened.file, format, package)?;
    let sheet_name = asset.locator.sheet.as_deref().ok_or_else(|| {
        stillflow_core::ConnectorError::invalid_configuration(
            "workbook asset is missing its sheet name",
        )
    })?;
    let sheet = workbook.load_sheet(sheet_name)?;
    let inspection = analyze_sheet(&sheet, config, context)?;
    let (schema, row_count) = if let Some(selection) = asset.locator.workbook_region {
        ensure_selection_inside_sheet(&sheet.range, selection.range)?;
        let prepared = prepare_schema(
            &sheet.range,
            sheet_name,
            asset.id,
            selection,
            None,
            None,
            context,
        )?;
        let rows = if prepared.data_rows_empty {
            0
        } else {
            u64::from(prepared.last_data_row - prepared.first_data_row) + 1
        };
        (prepared.schema, Some(rows))
    } else {
        (LogicalSchema::empty(), None)
    };

    let mut findings = Vec::new();
    if asset.locator.workbook_region.is_none() {
        findings.push(finding(
            "workbook.explicit_selection_required",
            "Preview and read require an explicit region and header selection from inspection output.",
            FindingSeverity::Info,
        ));
    }
    if inspection.region_candidates.len() > 1 {
        findings.push(finding(
            "workbook.ambiguous_regions",
            "The worksheet contains multiple data-region candidates.",
            FindingSeverity::Warning,
        ));
    }
    if inspection.region_candidates.is_empty() {
        findings.push(finding(
            "workbook.no_data_region",
            "The worksheet does not contain a populated data-region candidate.",
            FindingSeverity::Warning,
        ));
    }
    if inspection.analysis_truncated {
        findings.push(finding(
            "workbook.analysis_truncated",
            "Workbook analysis reached a configured row, column, or candidate bound.",
            FindingSeverity::Warning,
        ));
    }
    if inspection.formula_cells > 0 {
        findings.push(finding(
            "workbook.formula_cached_values",
            "Formula cells are present; previews use cached values and do not recalculate formulas.",
            FindingSeverity::Warning,
        ));
    }
    if !sheet.merged.is_empty() {
        findings.push(finding(
            "workbook.merged_cells",
            "Merged cells are present and may affect header or region interpretation.",
            FindingSeverity::Warning,
        ));
    }
    if !sheet.merge_metadata_available {
        findings.push(finding(
            "workbook.merge_metadata_unavailable",
            "Merged-cell metadata is unavailable for this workbook format.",
            FindingSeverity::Info,
        ));
    }
    if !matches!(
        inspection.sheet_visibility,
        WorkbookSheetVisibility::Visible
    ) {
        findings.push(finding(
            "workbook.hidden_sheet",
            "The selected worksheet is hidden in the workbook.",
            FindingSeverity::Warning,
        ));
    }
    findings.push(finding(
        "workbook.hidden_metadata_unavailable",
        "Hidden row and column metadata is unavailable for this format adapter.",
        FindingSeverity::Info,
    ));
    if asset
        .locator
        .workbook_region
        .is_some_and(|selection| matches!(selection.header, WorkbookHeaderSelection::NoHeader))
    {
        findings.push(finding(
            "workbook.no_header_selected",
            "The selected region uses generated column names because no header row was chosen.",
            FindingSeverity::Info,
        ));
    }

    context.ensure_active()?;
    Ok(AssetMetadata {
        schema,
        format: format.label().to_owned(),
        size_bytes: Some(opened.size_bytes),
        row_count,
        modified_at: opened.modified_at,
        findings,
        workbook: Some(inspection),
    })
}

fn finding(
    code: &'static str,
    message: &'static str,
    severity: FindingSeverity,
) -> InspectionFinding {
    InspectionFinding {
        code: code.to_owned(),
        message: message.to_owned(),
        severity,
    }
}
