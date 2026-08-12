use stillflow_core::{ConnectorError, ConnectorResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TabularFormat {
    Csv,
    Tsv,
    Json,
    Ndjson,
    Parquet,
}

impl TabularFormat {
    pub(crate) fn from_locator(locator: &str) -> ConnectorResult<Self> {
        let extension = locator
            .rsplit_once('.')
            .map(|(_, extension)| extension)
            .unwrap_or_default()
            .to_ascii_lowercase();
        match extension.as_str() {
            "csv" => Ok(Self::Csv),
            "tsv" => Ok(Self::Tsv),
            "json" => Ok(Self::Json),
            "jsonl" | "ndjson" => Ok(Self::Ndjson),
            "parquet" => Ok(Self::Parquet),
            _ => Err(ConnectorError::invalid_configuration(
                "asset extension is not supported by the local tabular connector",
            )),
        }
    }

    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Tsv => "tsv",
            Self::Json => "json",
            Self::Ndjson => "ndjson",
            Self::Parquet => "parquet",
        }
    }

    pub(crate) fn supports_file_name(name: &str) -> bool {
        Self::from_locator(name).is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_only_supported_extensions_case_insensitively() {
        assert_eq!(
            TabularFormat::from_locator("A.CSV").expect("CSV"),
            TabularFormat::Csv
        );
        assert_eq!(
            TabularFormat::from_locator("rows.jsonl").expect("JSONL"),
            TabularFormat::Ndjson
        );
        assert!(!TabularFormat::supports_file_name("archive.csv.gz"));
    }
}
