use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;

use quick_xml::events::{BytesStart, Event};
use quick_xml::name::QName;
use quick_xml::Reader as XmlReader;
use stillflow_core::{ConnectorError, ConnectorResult, ErrorCategory, RequestContext};
use zip::ZipArchive;

use crate::config::WorkbookConfig;
use crate::format::WorkbookFormat;

const MAX_PREFLIGHT_XML_BYTES: u64 = 64 * 1024 * 1024;
const MAX_ODS_ROWS: u64 = 1_048_576;
const MAX_ODS_COLUMNS: u64 = 16_384;

pub(crate) fn preflight(
    file: &File,
    format: WorkbookFormat,
    config: &WorkbookConfig,
    context: &RequestContext,
) -> ConnectorResult<()> {
    context.ensure_active()?;
    let size = file
        .metadata()
        .map_err(|_| {
            source_error(
                ErrorCategory::TransientSource,
                true,
                "workbook metadata could not be read",
            )
        })?
        .len();
    if size > config.max_workbook_bytes {
        return Err(invalid_data("workbook exceeds maxWorkbookBytes"));
    }
    if !format.is_zip_container() {
        return Ok(());
    }

    let clone = file.try_clone().map_err(|_| {
        source_error(
            ErrorCategory::TransientSource,
            true,
            "workbook handle could not be prepared for package inspection",
        )
    })?;
    let mut archive = ZipArchive::new(clone)
        .map_err(|_| invalid_data("workbook package is malformed or encrypted"))?;
    if archive.len() > config.max_archive_entries {
        return Err(invalid_data("workbook package exceeds maxArchiveEntries"));
    }

    let mut expanded = 0_u64;
    let mut ods_content = None;
    for index in 0..archive.len() {
        context.ensure_active()?;
        let entry = archive
            .by_index(index)
            .map_err(|_| invalid_data("workbook package entry is unreadable"))?;
        if entry.encrypted() {
            return Err(invalid_data("encrypted workbook packages are unsupported"));
        }
        if entry.enclosed_name().is_none() {
            return Err(invalid_data(
                "workbook package contains an unsafe entry path",
            ));
        }
        let size = entry.size();
        if size > config.max_expanded_archive_bytes {
            return Err(invalid_data(
                "workbook package entry exceeds the expansion bound",
            ));
        }
        expanded = expanded
            .checked_add(size)
            .ok_or_else(|| invalid_data("workbook package expansion size overflow"))?;
        if expanded > config.max_expanded_archive_bytes {
            return Err(invalid_data("workbook package exceeds the expansion bound"));
        }
        if format == WorkbookFormat::Ods && entry.name() == "content.xml" {
            ods_content = Some(index);
        }
    }

    if let Some(index) = ods_content {
        let mut entry = archive
            .by_index(index)
            .map_err(|_| invalid_data("ODS content entry is unreadable"))?;
        if entry.size() > MAX_PREFLIGHT_XML_BYTES {
            return Err(invalid_data(
                "ODS content entry exceeds the inspection bound",
            ));
        }
        let capacity = usize::try_from(entry.size())
            .map_err(|_| invalid_data("ODS content entry exceeds the platform range"))?;
        let mut bytes = Vec::with_capacity(capacity);
        entry
            .read_to_end(&mut bytes)
            .map_err(|_| invalid_data("ODS content entry could not be inspected"))?;
        validate_ods_repeats(&bytes, config.max_sheet_cells, context)?;
    }
    Ok(())
}

fn validate_ods_repeats(
    bytes: &[u8],
    max_cells: u64,
    context: &RequestContext,
) -> ConnectorResult<()> {
    let mut reader = XmlReader::from_reader(bytes);
    reader.config_mut().check_end_names = true;
    let mut buffer = Vec::new();
    let mut sheet = None;
    let mut row = None;

    loop {
        context.ensure_active()?;
        match reader
            .read_event_into(&mut buffer)
            .map_err(|_| invalid_data("ODS content XML is malformed"))?
        {
            Event::Start(tag) if tag.name() == QName(b"table:table") => {
                if sheet.replace(OdsSheetBounds::default()).is_some() {
                    return Err(invalid_data("ODS content contains nested sheets"));
                }
            }
            Event::Empty(tag) if tag.name() == QName(b"table:table") => {}
            Event::Start(tag) if tag.name() == QName(b"table:table-row") => {
                if sheet.is_some() {
                    if row.is_some() {
                        return Err(invalid_data("ODS content contains nested rows"));
                    }
                    row = Some(OdsOpenRow {
                        repeats: repeat_attribute(&tag, b"table:number-rows-repeated")?,
                        bounds: OdsRowBounds::default(),
                    });
                }
            }
            Event::Empty(tag) if tag.name() == QName(b"table:table-row") => {
                if let Some(sheet) = sheet.as_mut() {
                    let repeats = repeat_attribute(&tag, b"table:number-rows-repeated")?;
                    sheet.finish_row(OdsRowBounds::default(), repeats, max_cells)?;
                }
            }
            Event::Start(tag) | Event::Empty(tag)
                if tag.name() == QName(b"table:table-cell")
                    || tag.name() == QName(b"table:covered-table-cell") =>
            {
                if let Some(row) = row.as_mut() {
                    row.bounds.push_cell(
                        repeat_attribute(&tag, b"table:number-columns-repeated")?,
                        cell_is_materialized(&tag)?,
                    )?;
                }
            }
            Event::End(tag) if tag.name() == QName(b"table:table-row") => {
                if let (Some(sheet), Some(row)) = (sheet.as_mut(), row.take()) {
                    sheet.finish_row(row.bounds, row.repeats, max_cells)?;
                }
            }
            Event::End(tag) if tag.name() == QName(b"table:table") => {
                if row.is_some() {
                    return Err(invalid_data("ODS row is not closed"));
                }
                sheet = None;
            }
            Event::Eof => {
                if sheet.is_some() || row.is_some() {
                    return Err(invalid_data("ODS content XML is incomplete"));
                }
                break;
            }
            _ => {}
        }
        buffer.clear();
    }
    Ok(())
}

#[derive(Default)]
struct OdsSheetBounds {
    next_row: u64,
    first_materialized_row: Option<u64>,
    last_materialized_row: u64,
    first_materialized_column: Option<u64>,
    last_materialized_column: u64,
}

impl OdsSheetBounds {
    fn finish_row(
        &mut self,
        row: OdsRowBounds,
        repeats: u64,
        max_cells: u64,
    ) -> ConnectorResult<()> {
        if repeats == 0 {
            return Err(invalid_data("ODS row repeat must be positive"));
        }
        let start = self.next_row;
        self.next_row = self
            .next_row
            .checked_add(repeats)
            .filter(|value| *value <= MAX_ODS_ROWS)
            .ok_or_else(|| invalid_data("ODS row declarations exceed the format bound"))?;

        let (Some(first_column), Some(last_column)) =
            (row.first_materialized, row.last_materialized)
        else {
            return Ok(());
        };
        self.first_materialized_row.get_or_insert(start);
        self.last_materialized_row = self.next_row;
        self.first_materialized_column = Some(
            self.first_materialized_column
                .map_or(first_column, |existing| existing.min(first_column)),
        );
        self.last_materialized_column = self.last_materialized_column.max(last_column);

        let rows = self
            .last_materialized_row
            .checked_sub(
                self.first_materialized_row
                    .ok_or_else(|| invalid_data("ODS row range is invalid"))?,
            )
            .ok_or_else(|| invalid_data("ODS row range is invalid"))?;
        let columns = self
            .last_materialized_column
            .checked_sub(
                self.first_materialized_column
                    .ok_or_else(|| invalid_data("ODS column range is invalid"))?,
            )
            .ok_or_else(|| invalid_data("ODS column range is invalid"))?;
        let cells = rows
            .checked_mul(columns)
            .ok_or_else(|| invalid_data("ODS repeated-cell count overflow"))?;
        if cells > max_cells {
            return Err(invalid_data("ODS expanded cells exceed maxSheetCells"));
        }
        Ok(())
    }
}

#[derive(Default)]
struct OdsRowBounds {
    next_column: u64,
    first_materialized: Option<u64>,
    last_materialized: Option<u64>,
}

impl OdsRowBounds {
    fn push_cell(&mut self, repeats: u64, materialized: bool) -> ConnectorResult<()> {
        if repeats == 0 {
            return Err(invalid_data("ODS column repeat must be positive"));
        }
        let start = self.next_column;
        self.next_column = self
            .next_column
            .checked_add(repeats)
            .filter(|value| *value <= MAX_ODS_COLUMNS)
            .ok_or_else(|| invalid_data("ODS column declarations exceed the format bound"))?;
        if materialized {
            self.first_materialized.get_or_insert(start);
            self.last_materialized = Some(self.next_column);
        }
        Ok(())
    }
}

struct OdsOpenRow {
    repeats: u64,
    bounds: OdsRowBounds,
}

fn repeat_attribute(tag: &BytesStart<'_>, name: &[u8]) -> ConnectorResult<u64> {
    for attribute in tag.attributes() {
        let attribute =
            attribute.map_err(|_| invalid_data("ODS element attributes are malformed"))?;
        if attribute.key == QName(name) {
            return std::str::from_utf8(attribute.value.as_ref())
                .map_err(|_| invalid_data("ODS repeat attribute is not valid UTF-8"))?
                .parse::<u64>()
                .map_err(|_| invalid_data("ODS repeat attribute exceeds the numeric range"));
        }
    }
    Ok(1)
}

fn cell_is_materialized(tag: &BytesStart<'_>) -> ConnectorResult<bool> {
    const MATERIALIZED_ATTRIBUTES: &[&[u8]] = &[
        b"office:value",
        b"office:string-value",
        b"office:date-value",
        b"office:time-value",
        b"office:boolean-value",
        b"office:value-type",
        b"table:formula",
    ];
    for attribute in tag.attributes() {
        let attribute =
            attribute.map_err(|_| invalid_data("ODS element attributes are malformed"))?;
        if MATERIALIZED_ATTRIBUTES.contains(&attribute.key.as_ref()) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn invalid_data(message: &'static str) -> ConnectorError {
    source_error(ErrorCategory::InvalidData, false, message)
}

fn source_error(category: ErrorCategory, retryable: bool, message: &'static str) -> ConnectorError {
    ConnectorError::with_category(category, retryable, message, Vec::new(), BTreeMap::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_ods_repeat_expansion_above_the_product_bound() {
        let xml = br#"<table:table><table:table-row table:number-rows-repeated="2000"><table:table-cell table:number-columns-repeated="2000" office:value-type="float" office:value="1"/></table:table-row></table:table>"#;
        assert!(validate_ods_repeats(xml, 2_000_000, &RequestContext::default()).is_err());
    }

    #[test]
    fn accepts_small_ods_repeat_expansion() {
        let xml = br#"<table:table><table:table-row table:number-rows-repeated='2'><table:table-cell table:number-columns-repeated='3' office:value-type='float' office:value='1'/></table:table-row></table:table>"#;
        validate_ods_repeats(xml, 10, &RequestContext::default()).expect("small repeat");
    }

    #[test]
    fn ignores_trailing_empty_grid_padding_without_weakening_dimension_bounds() {
        let xml = br#"<table:table><table:table-row><table:table-cell office:value-type='float' office:value='1'/><table:table-cell table:number-columns-repeated='16383'/></table:table-row><table:table-row table:number-rows-repeated='1048575'><table:table-cell table:number-columns-repeated='16384'/></table:table-row></table:table>"#;
        validate_ods_repeats(xml, 1, &RequestContext::default()).expect("trailing padding");
    }
}
