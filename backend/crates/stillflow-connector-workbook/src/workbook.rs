use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufReader;

use calamine::{
    open_workbook_from_rs, Data, Dimensions, Ods, Range, Reader, SheetType, SheetVisible, Xls,
    Xlsb, Xlsx,
};
use stillflow_core::{ConnectorError, ConnectorResult, ErrorCategory, WorkbookSheetVisibility};

use crate::format::WorkbookFormat;

pub(crate) struct WorkbookReader {
    inner: ReaderKind,
}

enum ReaderKind {
    Xls(Xls<BufReader<File>>),
    Xlsx(Xlsx<BufReader<File>>),
    Xlsb(Xlsb<BufReader<File>>),
    Ods(Ods<BufReader<File>>),
}

#[derive(Debug, Clone)]
pub(crate) struct SheetDescriptor {
    pub(crate) name: String,
    pub(crate) ordinal: usize,
    pub(crate) visibility: WorkbookSheetVisibility,
}

pub(crate) struct LoadedSheet {
    pub(crate) range: Range<Data>,
    pub(crate) formulas: Range<String>,
    pub(crate) merged: Vec<Dimensions>,
    pub(crate) merge_metadata_available: bool,
    pub(crate) visibility: WorkbookSheetVisibility,
}

impl WorkbookReader {
    pub(crate) fn open(file: File, format: WorkbookFormat) -> ConnectorResult<Self> {
        let reader = BufReader::new(file);
        let inner = match format {
            WorkbookFormat::Xls => ReaderKind::Xls(
                open_workbook_from_rs(reader)
                    .map_err(|_| invalid_data("XLS workbook could not be decoded"))?,
            ),
            WorkbookFormat::Xlsx | WorkbookFormat::Xlsm => ReaderKind::Xlsx(
                open_workbook_from_rs(reader)
                    .map_err(|_| invalid_data("XLSX workbook could not be decoded"))?,
            ),
            WorkbookFormat::Xlsb => ReaderKind::Xlsb(
                open_workbook_from_rs(reader)
                    .map_err(|_| invalid_data("XLSB workbook could not be decoded"))?,
            ),
            WorkbookFormat::Ods => ReaderKind::Ods(
                open_workbook_from_rs(reader)
                    .map_err(|_| invalid_data("ODS workbook could not be decoded"))?,
            ),
        };
        Ok(Self { inner })
    }

    pub(crate) fn sheets(&self) -> Vec<SheetDescriptor> {
        self.sheet_metadata()
            .iter()
            .enumerate()
            .filter_map(|(ordinal, sheet)| {
                if sheet.typ != SheetType::WorkSheet {
                    return None;
                }
                Some(SheetDescriptor {
                    name: sheet.name.clone(),
                    ordinal,
                    visibility: visibility(sheet.visible),
                })
            })
            .collect()
    }

    pub(crate) fn load_sheet(&mut self, name: &str) -> ConnectorResult<LoadedSheet> {
        let visibility = self
            .sheets()
            .into_iter()
            .find(|sheet| sheet.name == name)
            .map(|sheet| sheet.visibility)
            .ok_or_else(|| {
                source_error(
                    ErrorCategory::NotFound,
                    false,
                    "workbook sheet was not found",
                )
            })?;
        let range = match &mut self.inner {
            ReaderKind::Xls(reader) => reader
                .worksheet_range(name)
                .map_err(|_| invalid_data("XLS worksheet data could not be decoded"))?,
            ReaderKind::Xlsx(reader) => reader
                .worksheet_range(name)
                .map_err(|_| invalid_data("XLSX worksheet data could not be decoded"))?,
            ReaderKind::Xlsb(reader) => reader
                .worksheet_range(name)
                .map_err(|_| invalid_data("XLSB worksheet data could not be decoded"))?,
            ReaderKind::Ods(reader) => reader
                .worksheet_range(name)
                .map_err(|_| invalid_data("ODS worksheet data could not be decoded"))?,
        };
        let formulas = match &mut self.inner {
            ReaderKind::Xls(reader) => reader
                .worksheet_formula(name)
                .map_err(|_| invalid_data("XLS worksheet formula metadata could not be decoded"))?,
            ReaderKind::Xlsx(reader) => reader.worksheet_formula(name).map_err(|_| {
                invalid_data("XLSX worksheet formula metadata could not be decoded")
            })?,
            ReaderKind::Xlsb(reader) => reader.worksheet_formula(name).map_err(|_| {
                invalid_data("XLSB worksheet formula metadata could not be decoded")
            })?,
            ReaderKind::Ods(reader) => reader
                .worksheet_formula(name)
                .map_err(|_| invalid_data("ODS worksheet formula metadata could not be decoded"))?,
        };
        let (merged, merge_metadata_available) = match &mut self.inner {
            ReaderKind::Xls(reader) => {
                (reader.worksheet_merge_cells(name).unwrap_or_default(), true)
            }
            ReaderKind::Xlsx(reader) => {
                let merged = reader
                    .worksheet_merge_cells(name)
                    .transpose()
                    .map_err(|_| invalid_data("worksheet merge metadata could not be decoded"))?
                    .unwrap_or_default();
                (merged, true)
            }
            ReaderKind::Xlsb(_) | ReaderKind::Ods(_) => (Vec::new(), false),
        };
        Ok(LoadedSheet {
            range,
            formulas,
            merged,
            merge_metadata_available,
            visibility,
        })
    }

    fn sheet_metadata(&self) -> &[calamine::Sheet] {
        match &self.inner {
            ReaderKind::Xls(reader) => reader.sheets_metadata(),
            ReaderKind::Xlsx(reader) => reader.sheets_metadata(),
            ReaderKind::Xlsb(reader) => reader.sheets_metadata(),
            ReaderKind::Ods(reader) => reader.sheets_metadata(),
        }
    }
}

fn visibility(value: SheetVisible) -> WorkbookSheetVisibility {
    match value {
        SheetVisible::Visible => WorkbookSheetVisibility::Visible,
        SheetVisible::Hidden => WorkbookSheetVisibility::Hidden,
        SheetVisible::VeryHidden => WorkbookSheetVisibility::VeryHidden,
    }
}

fn invalid_data(message: &'static str) -> ConnectorError {
    source_error(ErrorCategory::InvalidData, false, message)
}

fn source_error(category: ErrorCategory, retryable: bool, message: &'static str) -> ConnectorError {
    ConnectorError::with_category(category, retryable, message, Vec::new(), BTreeMap::new())
}
