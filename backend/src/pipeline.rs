use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap, HashSet},
    fs::File,
    path::Path,
    time::Instant,
};

use thiserror::Error;

use crate::models::{
    PipelineConfig, PipelineExecution, PipelineMetrics, PipelineNodeRequest, PreviewColumn,
};

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error("CSV error: {0}")]
    Csv(#[from] csv::Error),
    #[error("File error: {0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Invalid(String),
}

#[derive(Clone, Debug)]
pub struct TableData {
    pub headers: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

#[derive(Debug)]
pub struct PipelineOutput {
    pub table: TableData,
    pub executions: Vec<PipelineExecution>,
    pub total_duration: f64,
}

pub fn read_csv_file(path: &Path) -> Result<TableData, PipelineError> {
    let file = File::open(path)?;
    let mut reader = csv::ReaderBuilder::new()
        .flexible(false)
        .from_reader(file);

    let mut headers: Vec<String> = reader
        .headers()?
        .iter()
        .map(|header| header.trim().to_owned())
        .collect();

    if let Some(first) = headers.first_mut() {
        *first = first.trim_start_matches('\u{feff}').to_owned();
    }
    validate_headers(&headers)?;

    let rows = reader
        .records()
        .map(|record| {
            record.map(|record| record.iter().map(str::to_owned).collect::<Vec<_>>())
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(TableData { headers, rows })
}

pub fn write_csv_file(path: &Path, table: &TableData) -> Result<(), PipelineError> {
    let file = File::create(path)?;
    let mut writer = csv::WriterBuilder::new().from_writer(file);
    writer.write_record(&table.headers)?;
    for row in &table.rows {
        writer.write_record(row)?;
    }
    writer.flush()?;
    Ok(())
}

pub fn execute_pipeline(
    mut table: TableData,
    nodes: &[PipelineNodeRequest],
) -> Result<PipelineOutput, PipelineError> {
    if nodes.is_empty() {
        return Err(PipelineError::Invalid(
            "Pipeline must contain at least one enabled node".to_owned(),
        ));
    }

    let total_started = Instant::now();
    let mut executions = Vec::with_capacity(nodes.len());

    for (index, node) in nodes.iter().enumerate() {
        let started = Instant::now();
        let rows_in = table.rows.len();

        match node.node_type.as_str() {
            "source" | "export" => {}
            "filter" => filter_rows(&mut table, &node.config)?,
            "deduplicate" => deduplicate_rows(&mut table, &node.config)?,
            "normalize" => normalize_rows(&mut table, &node.config)?,
            other => {
                return Err(PipelineError::Invalid(format!(
                    "Unsupported node type: {other}"
                )));
            }
        }

        let rows_out = table.rows.len();
        executions.push(PipelineExecution {
            node_id: node.id.clone(),
            node_type: node.node_type.clone(),
            metrics: build_metrics(
                &table,
                rows_in,
                rows_out,
                &node.node_type,
                started.elapsed().as_secs_f64() * 1000.0,
            ),
            table_name: format!("stage_{}", index + 1),
        });
    }

    Ok(PipelineOutput {
        table,
        executions,
        total_duration: round_tenth(total_started.elapsed().as_secs_f64() * 1000.0),
    })
}

pub fn build_preview(
    table: &TableData,
    limit: usize,
) -> (Vec<PreviewColumn>, Vec<BTreeMap<String, String>>) {
    let columns = table
        .headers
        .iter()
        .enumerate()
        .map(|(index, name)| {
            let values: Vec<&String> = table
                .rows
                .iter()
                .filter_map(|row| row.get(index))
                .collect();
            let null_count = values
                .iter()
                .filter(|value| value.trim().is_empty())
                .count();
            let distinct_count = values
                .iter()
                .map(|value| value.trim())
                .filter(|value| !value.is_empty())
                .collect::<HashSet<_>>()
                .len();
            let whitespace_count = values
                .iter()
                .filter(|value| {
                    !value.trim().is_empty() && value.as_str() != value.trim()
                })
                .count();
            let column_type = infer_type(values.iter().copied().take(100));
            let numeric_values: Vec<f64> = if column_type == "number" {
                values
                    .iter()
                    .filter_map(|value| value.trim().parse::<f64>().ok())
                    .filter(|value| value.is_finite())
                    .collect()
            } else {
                Vec::new()
            };
            let minimum = numeric_values.iter().copied().reduce(f64::min);
            let maximum = numeric_values.iter().copied().reduce(f64::max);
            let average = (!numeric_values.is_empty()).then(|| {
                numeric_values.iter().sum::<f64>() / numeric_values.len() as f64
            });

            PreviewColumn {
                name: name.clone(),
                column_type,
                null_count,
                distinct_count,
                whitespace_count,
                minimum,
                maximum,
                average,
            }
        })
        .collect();

    let rows = table
        .rows
        .iter()
        .take(limit)
        .map(|row| {
            table
                .headers
                .iter()
                .cloned()
                .zip(row.iter().cloned())
                .collect()
        })
        .collect();

    (columns, rows)
}

pub fn build_preview_page(
    table: &TableData,
    offset: usize,
    limit: usize,
    sort_by: Option<&str>,
    sort_direction: Option<&str>,
    search: Option<&str>,
) -> Result<(Vec<BTreeMap<String, String>>, usize), PipelineError> {
    let search = search
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_lowercase);
    let mut rows: Vec<&Vec<String>> = table
        .rows
        .iter()
        .filter(|row| {
            search.as_ref().map_or(true, |query| {
                row.iter()
                    .any(|value| value.to_lowercase().contains(query))
            })
        })
        .collect();

    if let Some(column) = sort_by.map(str::trim).filter(|value| !value.is_empty()) {
        let index = column_index(table, column)?;
        let column_type = infer_type(table.rows.iter().filter_map(|row| row.get(index)).take(100));
        let descending = match sort_direction.unwrap_or("asc") {
            "asc" => false,
            "desc" => true,
            direction => {
                return Err(PipelineError::Invalid(format!(
                    "Unsupported preview sort direction: {direction}"
                )));
            }
        };
        rows.sort_by(|left, right| {
            compare_preview_values(
                left.get(index).map(String::as_str).unwrap_or_default(),
                right.get(index).map(String::as_str).unwrap_or_default(),
                &column_type,
                descending,
            )
        });
    }

    let filtered_rows = rows.len();
    let rows = rows
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|row| {
            table
                .headers
                .iter()
                .cloned()
                .zip(row.iter().cloned())
                .collect()
        })
        .collect();
    Ok((rows, filtered_rows))
}

pub fn count_duplicate_rows(table: &TableData) -> usize {
    let mut seen = HashSet::new();
    let mut duplicates = 0;
    for row in &table.rows {
        if !seen.insert(row) {
            duplicates += 1;
        }
    }
    duplicates
}

fn compare_preview_values(
    left: &str,
    right: &str,
    column_type: &str,
    descending: bool,
) -> Ordering {
    let left = left.trim();
    let right = right.trim();
    match (left.is_empty(), right.is_empty()) {
        (true, false) => return Ordering::Greater,
        (false, true) => return Ordering::Less,
        (true, true) => return Ordering::Equal,
        (false, false) => {}
    }

    let ordering = if column_type == "number" {
        match (left.parse::<f64>(), right.parse::<f64>()) {
            (Ok(left), Ok(right)) if left.is_finite() && right.is_finite() => {
                left.partial_cmp(&right).unwrap_or(Ordering::Equal)
            }
            _ => left.to_lowercase().cmp(&right.to_lowercase()),
        }
    } else {
        left.to_lowercase().cmp(&right.to_lowercase())
    };
    if descending {
        ordering.reverse()
    } else {
        ordering
    }
}

fn validate_headers(headers: &[String]) -> Result<(), PipelineError> {
    if headers.is_empty() {
        return Err(PipelineError::Invalid(
            "CSV must contain a header row".to_owned(),
        ));
    }

    let mut seen = HashSet::new();
    for header in headers {
        if header.is_empty() {
            return Err(PipelineError::Invalid(
                "CSV column names cannot be empty".to_owned(),
            ));
        }
        if !seen.insert(header.to_lowercase()) {
            return Err(PipelineError::Invalid(format!(
                "CSV contains a duplicate column: {header}"
            )));
        }
    }
    Ok(())
}

fn column_index(table: &TableData, requested: &str) -> Result<usize, PipelineError> {
    let requested = requested.trim();
    table
        .headers
        .iter()
        .position(|header| header.eq_ignore_ascii_case(requested))
        .ok_or_else(|| {
            PipelineError::Invalid(format!(
                "Column '{requested}' was not found. Available columns: {}",
                table.headers.join(", ")
            ))
        })
}

fn filter_rows(table: &mut TableData, config: &PipelineConfig) -> Result<(), PipelineError> {
    let index = column_index(table, &config.column)?;
    let operator = config.operator.trim();
    let value = config.value.trim();
    let treat_empty_as_match = config.null_handling == "Treat as match";

    let matches = |cell: &str| -> bool {
        let cell = cell.trim();
        if cell.is_empty() {
            // Emptiness operators describe the cell itself; every other
            // operator defers empty cells to the configured null handling.
            return match operator {
                "is empty" => true,
                _ if operator == "is not empty" || operator.is_empty() => false,
                _ => treat_empty_as_match,
            };
        }
        match operator {
            "is empty" => false,
            "equals" => cell.eq_ignore_ascii_case(value),
            "not equals" => !cell.eq_ignore_ascii_case(value),
            "contains" => cell
                .to_lowercase()
                .contains(value.to_lowercase().as_str()),
            "not contains" => !cell
                .to_lowercase()
                .contains(value.to_lowercase().as_str()),
            "greater than" | "less than" => {
                let (Ok(cell_num), Ok(value_num)) =
                    (cell.parse::<f64>(), value.parse::<f64>())
                else {
                    return false;
                };
                if operator == "greater than" {
                    cell_num > value_num
                } else {
                    cell_num < value_num
                }
            }
            // "is not empty" and legacy configs without an operator keep
            // every non-empty cell.
            _ => true,
        }
    };

    let remove_matching = config.mode == "Remove matching rows";
    table.rows.retain(|row| {
        let matched = matches(&row[index]);
        if remove_matching { !matched } else { matched }
    });
    Ok(())
}

fn deduplicate_rows(
    table: &mut TableData,
    config: &PipelineConfig,
) -> Result<(), PipelineError> {
    if config.column.trim().is_empty() {
        let rows = std::mem::take(&mut table.rows);
        table.rows = match config.strategy.as_str() {
            "Keep last" => {
                let mut seen = HashSet::new();
                let mut output = Vec::with_capacity(rows.len());
                for row in rows.into_iter().rev() {
                    if seen.insert(row.clone()) {
                        output.push(row);
                    }
                }
                output.reverse();
                output
            }
            "Keep first" | "Merge records" => {
                let mut seen = HashSet::new();
                rows.into_iter()
                    .filter(|row| seen.insert(row.clone()))
                    .collect()
            }
            strategy => {
                return Err(PipelineError::Invalid(format!(
                    "Unsupported deduplicate strategy: {strategy}"
                )));
            }
        };
        return Ok(());
    }

    let index = column_index(table, &config.column)?;
    let rows = std::mem::take(&mut table.rows);
    let remove_nulls = config.null_handling == "Remove null rows";
    let ignore_nulls = config.null_handling == "Ignore";

    table.rows = match config.strategy.as_str() {
        "Keep last" => keep_last(rows, index, remove_nulls, ignore_nulls),
        "Merge records" => merge_records(rows, index, remove_nulls, ignore_nulls),
        "Keep first" => keep_first(rows, index, remove_nulls, ignore_nulls),
        strategy => {
            return Err(PipelineError::Invalid(format!(
                "Unsupported deduplicate strategy: {strategy}"
            )));
        }
    };
    Ok(())
}

fn keep_first(
    rows: Vec<Vec<String>>,
    index: usize,
    remove_nulls: bool,
    ignore_nulls: bool,
) -> Vec<Vec<String>> {
    let mut seen: HashSet<Option<String>> = HashSet::new();
    let mut output = Vec::with_capacity(rows.len());

    for row in rows {
        let value = row[index].trim();
        if value.is_empty() && remove_nulls {
            continue;
        }
        if value.is_empty() && ignore_nulls {
            output.push(row);
            continue;
        }

        let key = (!value.is_empty()).then(|| value.to_owned());
        if seen.insert(key) {
            output.push(row);
        }
    }
    output
}

fn keep_last(
    rows: Vec<Vec<String>>,
    index: usize,
    remove_nulls: bool,
    ignore_nulls: bool,
) -> Vec<Vec<String>> {
    let mut seen: HashSet<Option<String>> = HashSet::new();
    let mut output = Vec::with_capacity(rows.len());

    for row in rows.into_iter().rev() {
        let value = row[index].trim();
        if value.is_empty() && remove_nulls {
            continue;
        }
        if value.is_empty() && ignore_nulls {
            output.push(row);
            continue;
        }

        let key = (!value.is_empty()).then(|| value.to_owned());
        if seen.insert(key) {
            output.push(row);
        }
    }
    output.reverse();
    output
}

fn merge_records(
    rows: Vec<Vec<String>>,
    index: usize,
    remove_nulls: bool,
    ignore_nulls: bool,
) -> Vec<Vec<String>> {
    let mut positions: HashMap<Option<String>, usize> = HashMap::new();
    let mut output: Vec<Vec<String>> = Vec::with_capacity(rows.len());

    for row in rows {
        let value = row[index].trim();
        if value.is_empty() && remove_nulls {
            continue;
        }
        if value.is_empty() && ignore_nulls {
            output.push(row);
            continue;
        }

        let key = (!value.is_empty()).then(|| value.to_owned());
        if let Some(position) = positions.get(&key).copied() {
            merge_row(&mut output[position], &row);
        } else {
            positions.insert(key, output.len());
            output.push(row);
        }
    }
    output
}

fn merge_row(target: &mut [String], incoming: &[String]) {
    for (current, value) in target.iter_mut().zip(incoming) {
        let value = value.trim();
        if value.is_empty() {
            continue;
        }
        if current.trim().is_empty() {
            *current = value.to_owned();
            continue;
        }
        if current.trim() != value
            && !current.split(" | ").any(|item| item.trim() == value)
        {
            current.push_str(" | ");
            current.push_str(value);
        }
    }
}

fn normalize_rows(table: &mut TableData, config: &PipelineConfig) -> Result<(), PipelineError> {
    let target_columns = if config.column.trim().is_empty() {
        (0..table.headers.len()).collect::<Vec<_>>()
    } else {
        vec![column_index(table, &config.column)?]
    };
    let email_columns: HashSet<usize> = target_columns
        .iter()
        .copied()
        .filter(|index| table.headers[*index].to_lowercase().contains("email"))
        .collect();

    for row in &mut table.rows {
        for index in &target_columns {
            let value = &mut row[*index];
            let normalized = value.trim();
            *value = if email_columns.contains(index) {
                normalized.to_lowercase()
            } else {
                normalized.to_owned()
            };
        }
    }

    if config.null_handling == "Remove null rows" && !config.column.trim().is_empty() {
        let index = column_index(table, &config.column)?;
        table.rows.retain(|row| !row[index].is_empty());
    }
    Ok(())
}

fn build_metrics(
    table: &TableData,
    rows_in: usize,
    rows_out: usize,
    node_type: &str,
    duration_ms: f64,
) -> PipelineMetrics {
    let mut null_cells = 0usize;
    let mut null_columns = HashSet::new();
    let mut bytes = table.headers.iter().map(String::len).sum::<usize>();

    for row in &table.rows {
        for (index, value) in row.iter().enumerate() {
            bytes += value.len();
            if value.trim().is_empty() {
                null_cells += 1;
                null_columns.insert(index);
            }
        }
    }

    let cell_count = rows_out.saturating_mul(table.headers.len());
    let missing = if cell_count == 0 {
        0.0
    } else {
        round_tenth((null_cells as f64 / cell_count as f64) * 100.0)
    };
    let duplicates = if node_type == "deduplicate" && rows_in > 0 {
        round_tenth(
            (rows_in.saturating_sub(rows_out) as f64 / rows_in as f64) * 100.0,
        )
    } else {
        0.0
    };

    PipelineMetrics {
        rows_in,
        rows_out,
        duplicates,
        missing,
        null_columns: null_columns.len(),
        quality_score: (100.0 - missing).clamp(0.0, 100.0).round() as u8,
        duration: round_tenth(duration_ms),
        memory: round_tenth(bytes as f64 / (1024.0 * 1024.0)),
    }
}

fn infer_type<'a>(values: impl Iterator<Item = &'a String>) -> String {
    let values: Vec<_> = values
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .collect();

    if values.is_empty() {
        return "string".to_owned();
    }
    if values.iter().all(|value| {
        value
            .parse::<f64>()
            .map(|number| number.is_finite())
            .unwrap_or(false)
    }) {
        return "number".to_owned();
    }
    if values
        .iter()
        .all(|value| matches!(value.to_lowercase().as_str(), "true" | "false"))
    {
        return "boolean".to_owned();
    }
    "string".to_owned()
}

fn round_tenth(value: f64) -> f64 {
    (value * 10.0).round() / 10.0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_table() -> TableData {
        TableData {
            headers: vec!["id".to_owned(), "email".to_owned(), "status".to_owned()],
            rows: vec![
                vec!["1".to_owned(), " A@EXAMPLE.COM ".to_owned(), "active".to_owned()],
                vec!["1".to_owned(), "a@example.com".to_owned(), "active".to_owned()],
                vec!["2".to_owned(), " B@EXAMPLE.COM ".to_owned(), "".to_owned()],
            ],
        }
    }

    #[test]
    fn executes_cleaning_nodes_in_order() {
        let nodes = vec![
            PipelineNodeRequest {
                id: "filter".to_owned(),
                node_type: "filter".to_owned(),
                config: PipelineConfig {
                    column: "status".to_owned(),
                    ..PipelineConfig::default()
                },
            },
            PipelineNodeRequest {
                id: "dedupe".to_owned(),
                node_type: "deduplicate".to_owned(),
                config: PipelineConfig {
                    column: "id".to_owned(),
                    ..PipelineConfig::default()
                },
            },
            PipelineNodeRequest {
                id: "normalize".to_owned(),
                node_type: "normalize".to_owned(),
                config: PipelineConfig::default(),
            },
        ];

        let output = execute_pipeline(test_table(), &nodes).expect("pipeline succeeds");

        assert_eq!(output.table.rows.len(), 1);
        assert_eq!(output.table.rows[0][1], "a@example.com");
        assert_eq!(output.executions[0].metrics.rows_out, 2);
        assert_eq!(output.executions[1].metrics.rows_out, 1);
    }

    #[test]
    fn preview_reports_nulls_and_distinct_values() {
        let table = test_table();
        let (columns, rows) = build_preview(&table, 2);

        assert_eq!(rows.len(), 2);
        assert_eq!(columns[2].null_count, 1);
        assert_eq!(columns[0].distinct_count, 2);
        assert_eq!(columns[1].whitespace_count, 2);
    }

    #[test]
    fn preview_page_filters_sorts_and_offsets() {
        let table = TableData {
            headers: vec!["id".to_owned(), "name".to_owned()],
            rows: vec![
                vec!["2".to_owned(), "Beta".to_owned()],
                vec!["1".to_owned(), "Alpha".to_owned()],
                vec!["3".to_owned(), "Gamma".to_owned()],
            ],
        };

        let (rows, filtered_rows) =
            build_preview_page(&table, 1, 1, Some("id"), Some("desc"), Some("a"))
                .expect("preview page");

        assert_eq!(filtered_rows, 3);
        assert_eq!(rows[0].get("id").map(String::as_str), Some("2"));
    }

    #[test]
    fn deduplicate_without_column_removes_identical_rows() {
        let mut table = test_table();
        table.rows.push(table.rows[0].clone());

        deduplicate_rows(&mut table, &PipelineConfig::default()).expect("deduplicate rows");

        assert_eq!(table.rows.len(), 3);
    }

    #[test]
    fn filter_applies_operator_mode_and_null_handling() {
        let mut table = test_table();

        // equals + keep matching: only "active" rows survive.
        filter_rows(
            &mut table,
            &PipelineConfig {
                column: "status".to_owned(),
                operator: "equals".to_owned(),
                value: "ACTIVE".to_owned(),
                null_handling: "Treat as non-match".to_owned(),
                ..PipelineConfig::default()
            },
        )
        .expect("filter equals");
        assert_eq!(table.rows.len(), 2);

        // remove matching rows inverts the rule.
        let mut inverted = test_table();
        filter_rows(
            &mut inverted,
            &PipelineConfig {
                column: "status".to_owned(),
                operator: "equals".to_owned(),
                value: "active".to_owned(),
                mode: "Remove matching rows".to_owned(),
                ..PipelineConfig::default()
            },
        )
        .expect("filter remove matching");
        assert_eq!(inverted.rows.len(), 1);
        assert_eq!(inverted.rows[0][0], "2");

        // numeric comparison on the id column.
        let mut numeric = test_table();
        filter_rows(
            &mut numeric,
            &PipelineConfig {
                column: "id".to_owned(),
                operator: "greater than".to_owned(),
                value: "1".to_owned(),
                ..PipelineConfig::default()
            },
        )
        .expect("filter greater than");
        assert_eq!(numeric.rows.len(), 1);
        assert_eq!(numeric.rows[0][0], "2");

        // empty cells defer to "Treat as match" for value operators.
        let mut nulls = test_table();
        filter_rows(
            &mut nulls,
            &PipelineConfig {
                column: "status".to_owned(),
                operator: "equals".to_owned(),
                value: "active".to_owned(),
                null_handling: "Treat as match".to_owned(),
                ..PipelineConfig::default()
            },
        )
        .expect("filter nulls as match");
        assert_eq!(nulls.rows.len(), 3);
    }

    #[test]
    fn csv_round_trip_preserves_shape() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("data.csv");
        let expected = test_table();

        write_csv_file(&path, &expected).expect("write csv");
        let actual = read_csv_file(&path).expect("read csv");

        assert_eq!(actual.headers, expected.headers);
        assert_eq!(actual.rows, expected.rows);
    }
}
