use std::path::Path;

use stillflow_core::{ConnectorError, ConnectorResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkbookFormat {
    Xls,
    Xlsx,
    Xlsm,
    Xlsb,
    Ods,
}

impl WorkbookFormat {
    pub(crate) fn from_locator(locator: &str) -> ConnectorResult<Self> {
        let extension = Path::new(locator)
            .extension()
            .and_then(|value| value.to_str())
            .map(str::to_ascii_lowercase);
        match extension.as_deref() {
            Some("xls") => Ok(Self::Xls),
            Some("xlsx") => Ok(Self::Xlsx),
            Some("xlsm") => Ok(Self::Xlsm),
            Some("xlsb") => Ok(Self::Xlsb),
            Some("ods") => Ok(Self::Ods),
            _ => Err(ConnectorError::invalid_configuration(
                "workbook asset has an unsupported file extension",
            )),
        }
    }

    pub(crate) fn supports_file_name(name: &str) -> bool {
        Self::from_locator(name).is_ok()
    }

    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Xls => "xls",
            Self::Xlsx => "xlsx",
            Self::Xlsm => "xlsm",
            Self::Xlsb => "xlsb",
            Self::Ods => "ods",
        }
    }

    pub(crate) const fn is_zip_container(self) -> bool {
        !matches!(self, Self::Xls)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_supported_suffixes_case_insensitively() {
        assert_eq!(
            WorkbookFormat::from_locator("book.XLSX").expect("xlsx"),
            WorkbookFormat::Xlsx
        );
        assert_eq!(
            WorkbookFormat::from_locator("book.ods").expect("ods"),
            WorkbookFormat::Ods
        );
        assert!(WorkbookFormat::from_locator("book.csv").is_err());
    }
}
