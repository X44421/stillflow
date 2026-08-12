use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;

use stillflow_core::{ConnectorError, ConnectorResult, ErrorCategory, RequestContext};
use zip::ZipArchive;

use crate::config::WorkbookConfig;
use crate::format::WorkbookFormat;

const MAX_PREFLIGHT_XML_BYTES: u64 = 64 * 1024 * 1024;

pub(crate) fn preflight(
    file: &File,
    format: WorkbookFormat,
    config: &WorkbookConfig,
    context: &RequestContext,
) -> ConnectorResult<()> {
    context.ensure_active()?;
    let size = file
        .metadata()
        .map_err(|_| source_error(ErrorCategory::TransientSource, true, "workbook metadata could not be read"))?
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
            return Err(invalid_data("workbook package contains an unsafe entry path"));
        }
        let size = entry.size();
        if size > config.max_expanded_archive_bytes {
            return Err(invalid_data("workbook package entry exceeds the expansion bound"));
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
            return Err(invalid_data("ODS content entry exceeds the inspection bound"));
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
    let row_open = b"<table:table-row";
    let row_close = b"</table:table-row>";
    let mut cursor = 0_usize;
    let mut total_cells = 0_u64;
    while let Some(relative_start) = find_bytes(
        bytes
            .get(cursor..)
            .ok_or_else(|| invalid_data("ODS inspection cursor is invalid"))?,
        row_open,
    ) {
        context.ensure_active()?;
        let start = cursor
            .checked_add(relative_start)
            .ok_or_else(|| invalid_data("ODS row position overflow"))?;
        let after_start = bytes
            .get(start..)
            .ok_or_else(|| invalid_data("ODS row start is invalid"))?;
        let Some(tag_end_relative) = after_start.iter().position(|byte| *byte == b'>') else {
            return Err(invalid_data("ODS row tag is incomplete"));
        };
        let tag_end = start
            .checked_add(tag_end_relative)
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| invalid_data("ODS row tag position overflow"))?;
        let row_tag = bytes
            .get(start..tag_end)
            .ok_or_else(|| invalid_data("ODS row tag boundary is invalid"))?;
        let row_repeats = attribute_u64(row_tag, b"table:number-rows-repeated")?.unwrap_or(1);
        if row_repeats == 0 || row_repeats > max_cells {
            return Err(invalid_data("ODS repeated rows exceed maxSheetCells"));
        }

        let remaining = bytes
            .get(tag_end..)
            .ok_or_else(|| invalid_data("ODS row body boundary is invalid"))?;
        let close_relative = find_bytes(remaining, row_close).unwrap_or_else(|| {
            remaining
                .iter()
                .position(|byte| *byte == b'<')
                .unwrap_or(remaining.len())
        });
        let row_body = remaining
            .get(..close_relative)
            .ok_or_else(|| invalid_data("ODS row body is invalid"))?;
        let columns = count_ods_columns(row_body, max_cells)?;
        let cells = row_repeats
            .checked_mul(columns)
            .ok_or_else(|| invalid_data("ODS repeated-cell count overflow"))?;
        total_cells = total_cells
            .checked_add(cells)
            .ok_or_else(|| invalid_data("ODS repeated-cell count overflow"))?;
        if total_cells > max_cells {
            return Err(invalid_data("ODS expanded cells exceed maxSheetCells"));
        }
        cursor = tag_end
            .checked_add(close_relative)
            .ok_or_else(|| invalid_data("ODS inspection cursor overflow"))?;
    }
    Ok(())
}

fn count_ods_columns(bytes: &[u8], max_cells: u64) -> ConnectorResult<u64> {
    let mut cursor = 0_usize;
    let mut columns = 0_u64;
    while let Some(relative) = find_bytes(
        bytes
            .get(cursor..)
            .ok_or_else(|| invalid_data("ODS cell cursor is invalid"))?,
        b"<table:",
    ) {
        let start = cursor
            .checked_add(relative)
            .ok_or_else(|| invalid_data("ODS cell position overflow"))?;
        let remaining = bytes
            .get(start..)
            .ok_or_else(|| invalid_data("ODS cell boundary is invalid"))?;
        let Some(end_relative) = remaining.iter().position(|byte| *byte == b'>') else {
            return Err(invalid_data("ODS cell tag is incomplete"));
        };
        let end = start
            .checked_add(end_relative)
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| invalid_data("ODS cell tag position overflow"))?;
        let tag = bytes
            .get(start..end)
            .ok_or_else(|| invalid_data("ODS cell tag boundary is invalid"))?;
        if tag.starts_with(b"<table:table-cell")
            || tag.starts_with(b"<table:covered-table-cell")
        {
            let repeated = attribute_u64(tag, b"table:number-columns-repeated")?.unwrap_or(1);
            if repeated == 0 || repeated > max_cells {
                return Err(invalid_data("ODS repeated columns exceed maxSheetCells"));
            }
            columns = columns
                .checked_add(repeated)
                .ok_or_else(|| invalid_data("ODS repeated-column count overflow"))?;
            if columns > max_cells {
                return Err(invalid_data("ODS row width exceeds maxSheetCells"));
            }
        }
        cursor = end;
    }
    Ok(columns)
}

fn attribute_u64(tag: &[u8], name: &[u8]) -> ConnectorResult<Option<u64>> {
    let Some(position) = find_bytes(tag, name) else {
        return Ok(None);
    };
    let mut cursor = position
        .checked_add(name.len())
        .ok_or_else(|| invalid_data("ODS attribute position overflow"))?;
    while tag.get(cursor).is_some_and(u8::is_ascii_whitespace) {
        cursor += 1;
    }
    if tag.get(cursor) != Some(&b'=') {
        return Err(invalid_data("ODS repeat attribute is malformed"));
    }
    cursor += 1;
    while tag.get(cursor).is_some_and(u8::is_ascii_whitespace) {
        cursor += 1;
    }
    let quote = *tag
        .get(cursor)
        .filter(|value| matches!(value, b'\'' | b'"'))
        .ok_or_else(|| invalid_data("ODS repeat attribute is malformed"))?;
    cursor += 1;
    let start = cursor;
    while tag.get(cursor).is_some_and(u8::is_ascii_digit) {
        cursor += 1;
    }
    if cursor == start || tag.get(cursor) != Some(&quote) {
        return Err(invalid_data("ODS repeat attribute is malformed"));
    }
    let value = std::str::from_utf8(
        tag.get(start..cursor)
            .ok_or_else(|| invalid_data("ODS repeat attribute boundary is invalid"))?,
    )
    .map_err(|_| invalid_data("ODS repeat attribute is not valid UTF-8"))?
    .parse::<u64>()
    .map_err(|_| invalid_data("ODS repeat attribute exceeds the numeric range"))?;
    Ok(Some(value))
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn invalid_data(message: &'static str) -> ConnectorError {
    source_error(ErrorCategory::InvalidData, false, message)
}

fn source_error(
    category: ErrorCategory,
    retryable: bool,
    message: &'static str,
) -> ConnectorError {
    ConnectorError::with_category(category, retryable, message, Vec::new(), BTreeMap::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_ods_repeat_expansion_above_the_product_bound() {
        let xml = br#"<table:table-row table:number-rows-repeated="2000"><table:table-cell table:number-columns-repeated="2000"/></table:table-row>"#;
        assert!(validate_ods_repeats(xml, 2_000_000, &RequestContext::default()).is_err());
    }

    #[test]
    fn accepts_small_ods_repeat_expansion() {
        let xml = br#"<table:table-row table:number-rows-repeated='2'><table:table-cell table:number-columns-repeated='3'/></table:table-row>"#;
        validate_ods_repeats(xml, 10, &RequestContext::default()).expect("small repeat");
    }
}
