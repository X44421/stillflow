use serde::{Deserialize, Serialize};

use crate::{ConnectorError, ConnectorResult};

/// Zero-based coordinate of one workbook cell.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CellCoordinate {
    pub row: u32,
    pub column: u32,
}

impl CellCoordinate {
    pub const fn new(row: u32, column: u32) -> Self {
        Self { row, column }
    }
}

/// Inclusive, zero-based rectangular workbook range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CellRange {
    pub start: CellCoordinate,
    pub end: CellCoordinate,
}

impl CellRange {
    pub fn try_new(start: CellCoordinate, end: CellCoordinate) -> ConnectorResult<Self> {
        let range = Self { start, end };
        range.validate()?;
        Ok(range)
    }

    pub fn validate(&self) -> ConnectorResult<()> {
        if self.start.row > self.end.row || self.start.column > self.end.column {
            return Err(ConnectorError::invalid_configuration(
                "workbook range endpoints are inverted",
            ));
        }
        self.area()?;
        Ok(())
    }

    pub fn row_count(&self) -> ConnectorResult<u64> {
        u64::from(self.end.row)
            .checked_sub(u64::from(self.start.row))
            .and_then(|distance| distance.checked_add(1))
            .ok_or_else(|| ConnectorError::invalid_configuration("workbook row range overflow"))
    }

    pub fn column_count(&self) -> ConnectorResult<u64> {
        u64::from(self.end.column)
            .checked_sub(u64::from(self.start.column))
            .and_then(|distance| distance.checked_add(1))
            .ok_or_else(|| {
                ConnectorError::invalid_configuration("workbook column range overflow")
            })
    }

    pub fn area(&self) -> ConnectorResult<u64> {
        self.row_count()?
            .checked_mul(self.column_count()?)
            .ok_or_else(|| ConnectorError::invalid_configuration("workbook range area overflow"))
    }

    pub const fn contains(&self, coordinate: CellCoordinate) -> bool {
        coordinate.row >= self.start.row
            && coordinate.row <= self.end.row
            && coordinate.column >= self.start.column
            && coordinate.column <= self.end.column
    }
}

/// Explicit interpretation of a selected workbook region's header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum WorkbookHeaderSelection {
    NoHeader,
    Row(u32),
}

/// Caller-authorized workbook range and header choice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkbookRegionSelection {
    pub range: CellRange,
    pub header: WorkbookHeaderSelection,
}

impl WorkbookRegionSelection {
    pub fn validate(&self) -> ConnectorResult<()> {
        self.range.validate()?;
        if let WorkbookHeaderSelection::Row(row) = self.header {
            if row < self.range.start.row || row > self.range.end.row {
                return Err(ConnectorError::invalid_configuration(
                    "workbook header row is outside the selected range",
                ));
            }
        }
        Ok(())
    }
}

/// Deterministic confidence bucket for workbook analysis candidates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CandidateConfidence {
    Low,
    Medium,
    High,
}

impl CandidateConfidence {
    pub const fn for_header_score(score: u8) -> Self {
        if score >= 80 {
            Self::High
        } else if score >= 50 {
            Self::Medium
        } else {
            Self::Low
        }
    }
}

/// Candidate header row produced by the workbook analyzer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkbookHeaderCandidate {
    pub row: u32,
    pub confidence: CandidateConfidence,
    pub score: u8,
}

impl WorkbookHeaderCandidate {
    pub fn validate(&self, range: &CellRange) -> ConnectorResult<()> {
        if self.score > 100 || self.confidence != CandidateConfidence::for_header_score(self.score)
        {
            return Err(ConnectorError::invalid_configuration(
                "workbook header candidate has an invalid score",
            ));
        }
        if !range.contains(CellCoordinate::new(self.row, range.start.column)) {
            return Err(ConnectorError::invalid_configuration(
                "workbook header candidate is outside its region",
            ));
        }
        Ok(())
    }
}

/// One deterministic rectangular data-region candidate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkbookRegionCandidate {
    pub range: CellRange,
    pub confidence: CandidateConfidence,
    pub non_empty_cells: u64,
    pub header_candidates: Vec<WorkbookHeaderCandidate>,
}

impl WorkbookRegionCandidate {
    pub fn validate(&self) -> ConnectorResult<()> {
        self.range.validate()?;
        if self.non_empty_cells == 0 || self.non_empty_cells > self.range.area()? {
            return Err(ConnectorError::invalid_configuration(
                "workbook region candidate has an invalid populated-cell count",
            ));
        }
        let mut previous = None;
        for candidate in &self.header_candidates {
            candidate.validate(&self.range)?;
            if previous.is_some_and(|row| row >= candidate.row) {
                return Err(ConnectorError::invalid_configuration(
                    "workbook header candidates are not strictly ordered",
                ));
            }
            previous = Some(candidate.row);
        }
        Ok(())
    }
}

/// Visibility reported by the workbook container.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum WorkbookSheetVisibility {
    Visible,
    Hidden,
    VeryHidden,
}

/// Structured workbook-specific inspection metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkbookInspection {
    pub sheet_visibility: WorkbookSheetVisibility,
    pub formula_cells: u64,
    pub merged_regions: Vec<CellRange>,
    pub hidden_rows: Vec<u32>,
    pub hidden_columns: Vec<u32>,
    pub region_candidates: Vec<WorkbookRegionCandidate>,
    pub analysis_truncated: bool,
}

impl WorkbookInspection {
    pub fn validate(&self) -> ConnectorResult<()> {
        validate_strictly_ordered(&self.hidden_rows, "hidden rows")?;
        validate_strictly_ordered(&self.hidden_columns, "hidden columns")?;

        let mut previous = None;
        for range in &self.merged_regions {
            range.validate()?;
            if previous.is_some_and(|prior| prior >= *range) {
                return Err(ConnectorError::invalid_configuration(
                    "workbook merged regions are not strictly ordered",
                ));
            }
            previous = Some(*range);
        }

        let mut previous = None;
        for candidate in &self.region_candidates {
            candidate.validate()?;
            if previous.is_some_and(|range| range >= candidate.range) {
                return Err(ConnectorError::invalid_configuration(
                    "workbook region candidates are not strictly ordered",
                ));
            }
            previous = Some(candidate.range);
        }
        Ok(())
    }
}

fn validate_strictly_ordered(values: &[u32], label: &'static str) -> ConnectorResult<()> {
    if values.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(ConnectorError::invalid_configuration(format!(
            "workbook {label} are not strictly ordered"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_ranges_and_explicit_headers() {
        let range = CellRange::try_new(CellCoordinate::new(2, 1), CellCoordinate::new(5, 3))
            .expect("range");
        assert_eq!(range.area().expect("area"), 12);
        WorkbookRegionSelection {
            range,
            header: WorkbookHeaderSelection::Row(2),
        }
        .validate()
        .expect("header");
        assert!(WorkbookRegionSelection {
            range,
            header: WorkbookHeaderSelection::Row(1),
        }
        .validate()
        .is_err());
        assert!(CellRange::try_new(
            CellCoordinate::new(0, 0),
            CellCoordinate::new(u32::MAX, u32::MAX),
        )
        .is_err());
    }

    #[test]
    fn validates_candidate_order_and_scores() {
        let range = CellRange::try_new(CellCoordinate::new(0, 0), CellCoordinate::new(2, 1))
            .expect("range");
        let inspection = WorkbookInspection {
            sheet_visibility: WorkbookSheetVisibility::Visible,
            formula_cells: 0,
            merged_regions: Vec::new(),
            hidden_rows: Vec::new(),
            hidden_columns: Vec::new(),
            region_candidates: vec![WorkbookRegionCandidate {
                range,
                confidence: CandidateConfidence::High,
                non_empty_cells: 6,
                header_candidates: vec![WorkbookHeaderCandidate {
                    row: 0,
                    confidence: CandidateConfidence::High,
                    score: 100,
                }],
            }],
            analysis_truncated: false,
        };
        inspection.validate().expect("inspection");
    }
}
