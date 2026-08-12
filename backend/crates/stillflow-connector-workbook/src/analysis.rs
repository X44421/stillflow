use std::collections::HashSet;

use calamine::{Data, DataType, Dimensions, Range};
use stillflow_core::{
    CandidateConfidence, CellCoordinate, CellRange, ConnectorError, ConnectorResult,
    RequestContext, WorkbookHeaderCandidate, WorkbookInspection, WorkbookRegionCandidate,
};

use crate::config::WorkbookConfig;
use crate::workbook::LoadedSheet;

pub(crate) fn analyze_sheet(
    sheet: &LoadedSheet,
    config: &WorkbookConfig,
    context: &RequestContext,
) -> ConnectorResult<WorkbookInspection> {
    context.ensure_active()?;
    enforce_sheet_bound(&sheet.range, config.max_sheet_cells)?;
    let merged_regions = dimensions_to_ranges(&sheet.merged)?;
    let formula_cells = count_formulas(&sheet.formulas, context)?;
    let (region_candidates, analysis_truncated) = find_regions(
        &sheet.range,
        &merged_regions,
        config.analysis_rows,
        config.analysis_columns,
        config.max_region_candidates,
        context,
    )?;
    let inspection = WorkbookInspection {
        sheet_visibility: sheet.visibility,
        formula_cells,
        merged_regions,
        hidden_rows: Vec::new(),
        hidden_columns: Vec::new(),
        region_candidates,
        analysis_truncated,
    };
    inspection.validate()?;
    Ok(inspection)
}

pub(crate) fn enforce_sheet_bound(range: &Range<Data>, maximum: u64) -> ConnectorResult<()> {
    let (rows, columns) = range.get_size();
    let rows = u64::try_from(rows)
        .map_err(|_| invalid_configuration("workbook row count exceeds the platform range"))?;
    let columns = u64::try_from(columns)
        .map_err(|_| invalid_configuration("workbook column count exceeds the platform range"))?;
    let area = rows
        .checked_mul(columns)
        .ok_or_else(|| invalid_configuration("workbook sheet area overflow"))?;
    if area > maximum {
        return Err(ConnectorError::with_category(
            stillflow_core::ErrorCategory::InvalidData,
            false,
            "workbook sheet exceeds maxSheetCells",
            Vec::new(),
            std::collections::BTreeMap::new(),
        ));
    }
    Ok(())
}

pub(crate) fn ensure_selection_inside_sheet(
    range: &Range<Data>,
    selected: CellRange,
) -> ConnectorResult<()> {
    selected.validate()?;
    let (Some(start), Some(end)) = (range.start(), range.end()) else {
        return Err(invalid_configuration(
            "an empty worksheet has no selectable region",
        ));
    };
    if selected.start.row < start.0
        || selected.start.column < start.1
        || selected.end.row > end.0
        || selected.end.column > end.1
    {
        return Err(invalid_configuration(
            "workbook region is outside the decoded sheet bounds",
        ));
    }
    Ok(())
}

pub(crate) fn cell_at(range: &Range<Data>, row: u32, column: u32) -> ConnectorResult<&Data> {
    let start = range
        .start()
        .ok_or_else(|| invalid_configuration("workbook sheet is empty"))?;
    let relative_row = row
        .checked_sub(start.0)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| invalid_configuration("workbook row is outside the decoded range"))?;
    let relative_column = column
        .checked_sub(start.1)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| invalid_configuration("workbook column is outside the decoded range"))?;
    range
        .get((relative_row, relative_column))
        .ok_or_else(|| invalid_configuration("workbook cell is outside the decoded range"))
}

fn count_formulas(range: &Range<String>, context: &RequestContext) -> ConnectorResult<u64> {
    let mut count = 0_u64;
    for (index, (_, _, formula)) in range.used_cells().enumerate() {
        if index % 1024 == 0 {
            context.ensure_active()?;
        }
        if !formula.is_empty() {
            count = count
                .checked_add(1)
                .ok_or_else(|| invalid_configuration("workbook formula count overflow"))?;
        }
    }
    Ok(count)
}

fn dimensions_to_ranges(dimensions: &[Dimensions]) -> ConnectorResult<Vec<CellRange>> {
    let mut ranges = dimensions
        .iter()
        .map(|dimension| {
            CellRange::try_new(
                CellCoordinate::new(dimension.start.0, dimension.start.1),
                CellCoordinate::new(dimension.end.0, dimension.end.1),
            )
        })
        .collect::<ConnectorResult<Vec<_>>>()?;
    ranges.sort_unstable();
    ranges.dedup();
    Ok(ranges)
}

fn find_regions(
    range: &Range<Data>,
    merged: &[CellRange],
    max_rows: usize,
    max_columns: usize,
    max_candidates: usize,
    context: &RequestContext,
) -> ConnectorResult<(Vec<WorkbookRegionCandidate>, bool)> {
    let (height, width) = range.get_size();
    let analysis_height = height.min(max_rows);
    let analysis_width = width.min(max_columns);
    let mut truncated = height > analysis_height || width > analysis_width;
    if analysis_height == 0 || analysis_width == 0 {
        return Ok((Vec::new(), truncated));
    }
    let start = range
        .start()
        .ok_or_else(|| invalid_configuration("workbook range start is unavailable"))?;

    let mut populated_rows = vec![false; analysis_height];
    for (row_index, populated) in populated_rows.iter_mut().enumerate() {
        if row_index % 256 == 0 {
            context.ensure_active()?;
        }
        *populated = (0..analysis_width).any(|column| {
            range
                .get((row_index, column))
                .is_some_and(|cell| !cell.is_empty())
        });
    }
    let row_bands = true_bands(&populated_rows);
    let mut candidates = Vec::new();
    for (row_start, row_end) in row_bands {
        context.ensure_active()?;
        let mut populated_columns = vec![false; analysis_width];
        for (column, populated) in populated_columns.iter_mut().enumerate() {
            *populated = (row_start..=row_end).any(|row| {
                range
                    .get((row, column))
                    .is_some_and(|cell| !cell.is_empty())
            });
        }
        for (column_start, column_end) in true_bands(&populated_columns) {
            let Some((trimmed, non_empty)) =
                trim_region(range, start, row_start, row_end, column_start, column_end)?
            else {
                continue;
            };
            let confidence = region_confidence(range, start, trimmed)?;
            let header_candidates = header_candidates(range, start, trimmed, merged)?;
            candidates.push(WorkbookRegionCandidate {
                range: trimmed,
                confidence,
                non_empty_cells: non_empty,
                header_candidates,
            });
        }
    }
    candidates.sort_by_key(|candidate| candidate.range);
    candidates.dedup_by_key(|candidate| candidate.range);
    if candidates.len() > max_candidates {
        candidates.truncate(max_candidates);
        truncated = true;
    }
    Ok((candidates, truncated))
}

fn true_bands(values: &[bool]) -> Vec<(usize, usize)> {
    let mut bands = Vec::new();
    let mut start = None;
    for (index, value) in values.iter().copied().enumerate() {
        match (start, value) {
            (None, true) => start = Some(index),
            (Some(first), false) => {
                bands.push((first, index - 1));
                start = None;
            }
            _ => {}
        }
    }
    if let Some(first) = start {
        bands.push((first, values.len() - 1));
    }
    bands
}

fn trim_region(
    range: &Range<Data>,
    absolute_start: (u32, u32),
    row_start: usize,
    row_end: usize,
    column_start: usize,
    column_end: usize,
) -> ConnectorResult<Option<(CellRange, u64)>> {
    let mut min_row = usize::MAX;
    let mut min_column = usize::MAX;
    let mut max_row = 0_usize;
    let mut max_column = 0_usize;
    let mut non_empty = 0_u64;
    for row in row_start..=row_end {
        for column in column_start..=column_end {
            if range
                .get((row, column))
                .is_some_and(|cell| !cell.is_empty())
            {
                min_row = min_row.min(row);
                min_column = min_column.min(column);
                max_row = max_row.max(row);
                max_column = max_column.max(column);
                non_empty = non_empty.checked_add(1).ok_or_else(|| {
                    invalid_configuration("workbook populated-cell count overflow")
                })?;
            }
        }
    }
    if non_empty == 0 {
        return Ok(None);
    }
    let range = CellRange::try_new(
        CellCoordinate::new(
            add_index(
                absolute_start.0,
                min_row,
                "workbook row coordinate overflow",
            )?,
            add_index(
                absolute_start.1,
                min_column,
                "workbook column coordinate overflow",
            )?,
        ),
        CellCoordinate::new(
            add_index(
                absolute_start.0,
                max_row,
                "workbook row coordinate overflow",
            )?,
            add_index(
                absolute_start.1,
                max_column,
                "workbook column coordinate overflow",
            )?,
        ),
    )?;
    Ok(Some((range, non_empty)))
}

fn region_confidence(
    range: &Range<Data>,
    absolute_start: (u32, u32),
    candidate: CellRange,
) -> ConnectorResult<CandidateConfidence> {
    if candidate.row_count()? < 2 {
        return Ok(CandidateConfidence::Low);
    }
    let mut sparse = false;
    for row in candidate.start.row..=candidate.end.row {
        sparse |= !(candidate.start.column..=candidate.end.column).any(|column| {
            relative_cell(range, absolute_start, row, column).is_some_and(|cell| !cell.is_empty())
        });
    }
    for column in candidate.start.column..=candidate.end.column {
        sparse |= !(candidate.start.row..=candidate.end.row).any(|row| {
            relative_cell(range, absolute_start, row, column).is_some_and(|cell| !cell.is_empty())
        });
    }
    Ok(if sparse {
        CandidateConfidence::Medium
    } else {
        CandidateConfidence::High
    })
}

fn header_candidates(
    range: &Range<Data>,
    absolute_start: (u32, u32),
    candidate: CellRange,
    merged: &[CellRange],
) -> ConnectorResult<Vec<WorkbookHeaderCandidate>> {
    let mut results = Vec::new();
    for row in candidate.start.row..=candidate.end.row {
        if results.len() == 5 {
            break;
        }
        let cells = (candidate.start.column..=candidate.end.column)
            .filter_map(|column| relative_cell(range, absolute_start, row, column))
            .collect::<Vec<_>>();
        let populated = cells
            .iter()
            .copied()
            .filter(|cell| !cell.is_empty())
            .collect::<Vec<_>>();
        if populated.is_empty() {
            continue;
        }
        let mut score = 0_u8;
        if populated.len() >= 2 && populated.iter().all(|cell| is_text(cell)) {
            score += 40;
        }
        let unique = populated
            .iter()
            .map(|cell| cell.to_string())
            .collect::<HashSet<_>>()
            .len();
        if unique == populated.len() {
            score += 20;
        }
        if has_type_contrast(range, absolute_start, candidate, row)? {
            score += 20;
        }
        if populated.len().saturating_mul(4) >= cells.len().saturating_mul(3) {
            score += 10;
        }
        if !merged.iter().any(|merge| {
            row >= merge.start.row
                && row <= merge.end.row
                && candidate.start.column <= merge.end.column
                && candidate.end.column >= merge.start.column
        }) {
            score += 10;
        }
        results.push(WorkbookHeaderCandidate {
            row,
            confidence: CandidateConfidence::for_header_score(score),
            score,
        });
    }
    Ok(results)
}

fn has_type_contrast(
    range: &Range<Data>,
    absolute_start: (u32, u32),
    candidate: CellRange,
    header_row: u32,
) -> ConnectorResult<bool> {
    let mut populated = 0_usize;
    let mut contrasted = 0_usize;
    for column in candidate.start.column..=candidate.end.column {
        let Some(header) = relative_cell(range, absolute_start, header_row, column) else {
            continue;
        };
        if header.is_empty() {
            continue;
        }
        populated += 1;
        if !is_text(header) {
            continue;
        }
        let mut text = 0_usize;
        let mut non_text = 0_usize;
        for row in header_row.saturating_add(1)..=candidate.end.row {
            if text + non_text == 10 {
                break;
            }
            let Some(cell) = relative_cell(range, absolute_start, row, column) else {
                continue;
            };
            if cell.is_empty() {
                continue;
            }
            if is_text(cell) {
                text += 1;
            } else {
                non_text += 1;
            }
        }
        if non_text > text {
            contrasted += 1;
        }
    }
    Ok(populated > 0 && contrasted.saturating_mul(2) >= populated)
}

fn is_text(cell: &Data) -> bool {
    matches!(cell, Data::String(_))
}

fn relative_cell<'a>(
    range: &'a Range<Data>,
    absolute_start: (u32, u32),
    row: u32,
    column: u32,
) -> Option<&'a Data> {
    let row = usize::try_from(row.checked_sub(absolute_start.0)?).ok()?;
    let column = usize::try_from(column.checked_sub(absolute_start.1)?).ok()?;
    range.get((row, column))
}

fn add_index(base: u32, offset: usize, message: &'static str) -> ConnectorResult<u32> {
    let offset = u32::try_from(offset).map_err(|_| invalid_configuration(message))?;
    base.checked_add(offset)
        .ok_or_else(|| invalid_configuration(message))
}

fn invalid_configuration(message: &'static str) -> ConnectorError {
    ConnectorError::invalid_configuration(message)
}

#[cfg(test)]
mod tests {
    use calamine::Cell;
    use stillflow_core::WorkbookSheetVisibility;

    use super::*;

    fn sheet(cells: Vec<Cell<Data>>) -> LoadedSheet {
        LoadedSheet {
            range: Range::from_sparse(cells),
            formulas: Range::empty(),
            merged: Vec::new(),
            merge_metadata_available: true,
            visibility: WorkbookSheetVisibility::Visible,
        }
    }

    fn config(max_region_candidates: usize) -> WorkbookConfig {
        WorkbookConfig {
            allowed_roots: Vec::new(),
            max_discovery_depth: 16,
            max_discovered_assets: 100,
            max_workbook_bytes: 1024,
            max_archive_entries: 10,
            max_expanded_archive_bytes: 1024,
            max_sheet_cells: 100,
            max_region_candidates,
            analysis_rows: 10,
            analysis_columns: 10,
        }
    }

    #[test]
    fn splits_regions_on_blank_rows_and_columns() {
        let sheet = sheet(vec![
            Cell::new((0, 0), Data::String("a".into())),
            Cell::new((1, 0), Data::Int(1)),
            Cell::new((0, 2), Data::String("b".into())),
            Cell::new((1, 2), Data::Int(2)),
            Cell::new((3, 0), Data::String("c".into())),
            Cell::new((4, 0), Data::Int(3)),
        ]);
        let inspection =
            analyze_sheet(&sheet, &config(10), &RequestContext::default()).expect("analysis");
        assert_eq!(inspection.region_candidates.len(), 3);
        assert_eq!(
            inspection.region_candidates[0].range.start,
            CellCoordinate::new(0, 0)
        );
        assert_eq!(
            inspection.region_candidates[1].range.start,
            CellCoordinate::new(0, 2)
        );
        assert_eq!(
            inspection.region_candidates[2].range.start,
            CellCoordinate::new(3, 0)
        );
    }

    #[test]
    fn scores_headers_exactly_and_accounts_for_merged_rows() {
        let mut sheet = sheet(vec![
            Cell::new((0, 0), Data::String("name".into())),
            Cell::new((0, 1), Data::String("value".into())),
            Cell::new((1, 0), Data::Int(1)),
            Cell::new((1, 1), Data::Float(2.0)),
        ]);
        let inspection =
            analyze_sheet(&sheet, &config(10), &RequestContext::default()).expect("analysis");
        let header = &inspection.region_candidates[0].header_candidates[0];
        assert_eq!(header.score, 100);
        assert_eq!(header.confidence, CandidateConfidence::High);

        sheet.merged = vec![Dimensions::new((0, 0), (0, 1))];
        let inspection = analyze_sheet(&sheet, &config(10), &RequestContext::default())
            .expect("merged analysis");
        let header = &inspection.region_candidates[0].header_candidates[0];
        assert_eq!(header.score, 90);
        assert_eq!(inspection.merged_regions.len(), 1);
    }

    #[test]
    fn marks_title_only_regions_low_and_caps_ambiguous_candidates() {
        let title = sheet(vec![Cell::new((0, 0), Data::String("title".into()))]);
        let inspection =
            analyze_sheet(&title, &config(10), &RequestContext::default()).expect("title analysis");
        assert_eq!(
            inspection.region_candidates[0].confidence,
            CandidateConfidence::Low
        );

        let ambiguous = sheet(vec![
            Cell::new((0, 0), Data::String("a".into())),
            Cell::new((0, 2), Data::String("b".into())),
            Cell::new((0, 4), Data::String("c".into())),
        ]);
        let inspection = analyze_sheet(&ambiguous, &config(2), &RequestContext::default())
            .expect("bounded analysis");
        assert_eq!(inspection.region_candidates.len(), 2);
        assert!(inspection.analysis_truncated);
    }

    #[test]
    fn counts_formula_presence_without_exposing_formula_text() {
        let mut sheet = sheet(vec![Cell::new((0, 0), Data::Int(2))]);
        sheet.formulas = Range::from_sparse(vec![Cell::new((0, 0), "A2+1".to_owned())]);
        let inspection = analyze_sheet(&sheet, &config(10), &RequestContext::default())
            .expect("formula analysis");
        assert_eq!(inspection.formula_cells, 1);
    }
}
