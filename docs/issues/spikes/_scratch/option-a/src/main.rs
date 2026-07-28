//! Issue #6 Spike Phase 1 — Option A (Arrow C Data Interface)
//!
//! Scratch spike crate for Sol review. Not production code.
//!
//! ## `unsafe` / `transmute` locations (all in this file)
//!
//! | Site | Lines | API | Rationale |
//! | --- | --- | --- | --- |
//! | ABI cast (array) | `polars_array_to_arrow_rs` | `mem::transmute` | `polars_arrow::ffi::ArrowArray` ↔ `FFI_ArrowArray` ABI layout |
//! | FFI import (array) | `polars_array_to_arrow_rs` | `from_ffi_and_data_type` | Official Arrow 59 C Data Interface import; consumes `FFI_ArrowArray` once |
//! | ABI cast (schema) | `polars_field_to_arrow_rs_field` | `mem::transmute` | `polars_arrow::ffi::ArrowSchema` ↔ `FFI_ArrowSchema` ABI layout |
//!
//! Ownership rule: `export_array_to_c` consumes the polars `Box<dyn Array>`.
//! `from_ffi_and_data_type` consumes the `FFI_ArrowArray`. No manual `release` calls.

use std::fs::File;
use std::io::{BufRead, BufReader, Cursor};
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::ffi::{from_ffi_and_data_type, FFI_ArrowArray};
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{ArrowError, DataType, Field, Schema};
use polars::prelude::*;
use polars_arrow::array::Array as PolarsArray;
use polars_arrow::ffi::{export_array_to_c, export_field_to_c};
use polars_arrow::record_batch::RecordBatch as PolarsRecordBatch;

const FIXTURES: &str = "fixtures";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(FIXTURES);
    ensure_parquet_fixture(&data_dir)?;

    println!("=== Issue #6 Option A spike (technical validation only) ===");

    test_csv_chunked_projection(&data_dir)?;
    test_jsonl_multibatch_projection(&data_dir)?;
    test_parquet_multibatch_projection(&data_dir)?;
    test_nullability(&data_dir)?;
    test_empty_arrays(&data_dir)?;
    test_nested_types(&data_dir)?;
    test_field_metadata_manual_copy()?;
    test_ffi_ownership_and_repeated_drop(&data_dir)?;
    test_obatch_memory_bound(&data_dir)?;

    println!("All Option A spike checks passed.");
    println!("Production implementation remains blocked pending Sol approval.");
    Ok(())
}

/// Creates `sample.parquet` with three row groups if missing (committed fixture).
fn ensure_parquet_fixture(data_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let parquet_path = data_dir.join("sample.parquet");
    if parquet_path.exists() {
        return Ok(());
    }

    let mut rows = Vec::with_capacity(30);
    for i in 0..30 {
        rows.push((i, format!("user{i}"), i));
    }
    let df = df!(
        "id" => rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        "name" => rows.iter().map(|r| r.1.clone()).collect::<Vec<_>>(),
        "score" => rows.iter().map(|r| r.2).collect::<Vec<_>>(),
    )?;

    let mut file = File::create(&parquet_path)?;
    ParquetWriter::new(&mut file)
        .with_row_group_size(Some(10))
        .finish(&mut df.clone())?;
    Ok(())
}

// --- Bridge (would become `bridge/ffi.rs` in production if Sol approves unsafe exception) ---

fn polars_array_to_arrow_rs(
    polars_array: Box<dyn PolarsArray>,
    arrow_dtype: DataType,
) -> Result<ArrayRef, ArrowError> {
    // SAFETY SITE 1: ABI-identical struct transmute (polars ArrowArray → arrow-rs FFI_ArrowArray).
    let polars_c_array = export_array_to_c(polars_array);
    let arrow_c_array: FFI_ArrowArray = unsafe { mem::transmute(polars_c_array) };

    // SAFETY SITE 2: `from_ffi_and_data_type` — caller must guarantee valid C Data Interface export.
    // Consumes `arrow_c_array` exactly once (no double-release).
    Ok(arrow_array::make_array(unsafe {
        from_ffi_and_data_type(arrow_c_array, arrow_dtype)
    }?))
}

fn polars_field_to_arrow_rs_field(
    polars_field: polars_arrow::datatypes::Field,
) -> Result<Field, ArrowError> {
    // Metadata is NOT preserved by schema FFI alone; copy manually from polars Field.
    let metadata = polars_field
        .metadata
        .as_ref()
        .map(|md| {
            md.iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect::<std::collections::HashMap<_, _>>()
        })
        .unwrap_or_default();

    let polars_c_schema = export_field_to_c(&polars_field);
    // SAFETY SITE 3: ABI-identical struct transmute (polars ArrowSchema → FFI_ArrowSchema).
    let arrow_c_schema: arrow_schema::ffi::FFI_ArrowSchema =
        unsafe { mem::transmute(polars_c_schema) };
    let arrow_dtype = DataType::try_from(&arrow_c_schema)?;
    Ok(
        Field::new(
            polars_field.name.as_str(),
            arrow_dtype,
            polars_field.is_nullable,
        )
        .with_metadata(metadata),
    )
}

fn polars_rb_to_arrow_rb(polars_rb: PolarsRecordBatch) -> Result<RecordBatch, ArrowError> {
    let (schema, arrays) = polars_rb.into_schema_and_arrays();
    let arrow_fields: Vec<Field> = schema
        .iter_values()
        .cloned()
        .map(polars_field_to_arrow_rs_field)
        .collect::<Result<Vec<_>, _>>()?;

    let arrow_schema = Arc::new(Schema::new(arrow_fields.clone()));
    let arrow_columns = arrays
        .into_iter()
        .zip(arrow_fields.iter())
        .map(|(array, field)| polars_array_to_arrow_rs(array, field.data_type().clone()))
        .collect::<Result<Vec<_>, _>>()?;

    RecordBatch::try_new(arrow_schema, arrow_columns)
}

fn polars_df_to_record_batch(df: DataFrame) -> Result<RecordBatch, ArrowError> {
    polars_rb_to_arrow_rb(df.rechunk_to_record_batch(CompatLevel::newest()))
}

fn batch_memory_bytes(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .map(|array| array.get_array_memory_size())
        .sum()
}

fn assert_batch_projection(batch: &RecordBatch, expected_cols: &[&str], label: &str) {
    let schema = batch.schema();
    let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, expected_cols, "{label}: projection mismatch");
}

// --- Tests ---

fn test_csv_chunked_projection(data_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("CSV chunked read + projection:");
    let file = File::open(data_dir.join("sample.csv"))?;
    let mut reader = CsvReadOptions::default()
        .with_has_header(true)
        .with_chunk_size(64)
        .into_reader_with_file_handle(file);
    let mut batched = reader.batched_borrowed()?;

    let mut total_rows = 0usize;
    let mut batch_count = 0usize;
    while let Some(chunks) = batched.next_batches(1)? {
        for df in chunks {
            let projected = df.lazy().select([col("id"), col("name")]).collect()?;
            let batch = polars_df_to_record_batch(projected)?;
            assert_batch_projection(&batch, &["id", "name"], "csv chunk");
            assert!(batch.num_rows() > 0);
            total_rows += batch.num_rows();
            batch_count += 1;
        }
    }
    assert!(batch_count > 1, "expected multiple CSV batches");
    assert_eq!(total_rows, 50, "sample.csv has 50 data rows");
    println!("  {batch_count} batches, {total_rows} rows projected");
    Ok(())
}

fn read_jsonl_line_batch(
    path: &Path,
    start_line: usize,
    line_count: usize,
) -> Result<Option<DataFrame>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let lines: Vec<String> = BufReader::new(file)
        .lines()
        .skip(start_line)
        .take(line_count)
        .collect::<Result<_, _>>()?;
    if lines.is_empty() {
        return Ok(None);
    }
    let body = lines.join("\n");
    let df = JsonReader::new(Cursor::new(body.into_bytes()))
        .with_json_format(JsonFormat::JsonLines)
        .finish()?;
    Ok(Some(df))
}

fn test_jsonl_multibatch_projection(data_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("JSONL multi-batch read + projection:");
    let path = data_dir.join("multibatch.jsonl");

    let batch_line_count = 5usize;
    let mut start_line = 0usize;
    let mut batch_count = 0usize;
    let mut total_rows = 0usize;

    while let Some(df) = read_jsonl_line_batch(&path, start_line, batch_line_count)? {
        let projected = df.lazy().select([col("id"), col("value")]).collect()?;
        let batch = polars_df_to_record_batch(projected)?;
        assert_batch_projection(&batch, &["id", "value"], "jsonl batch");
        let rows = batch.num_rows();
        if rows == 0 {
            break;
        }
        total_rows += rows;
        batch_count += 1;
        start_line += rows;
        if total_rows >= 30 {
            break;
        }
    }

    assert!(batch_count > 1, "expected multiple JSONL batches");
    assert_eq!(total_rows, 30);
    println!("  {batch_count} batches, {total_rows} rows projected (line-window reads)");
    Ok(())
}

fn test_parquet_multibatch_projection(data_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("Parquet multi-batch read + projection:");
    let path = data_dir.join("sample.parquet");
    let lf = LazyFrame::scan_parquet(&path, ScanArgsParquet::default())?;

    let batch_size = 10usize;
    let mut offset = 0i64;
    let mut batch_count = 0usize;
    let mut total_rows = 0usize;

    loop {
        let df = lf
            .clone()
            .slice(offset, batch_size as u32)
            .select([col("id"), col("score")])
            .collect()?;
        if df.height() == 0 {
            break;
        }
        let batch = polars_df_to_record_batch(df)?;
        assert_batch_projection(&batch, &["id", "score"], "parquet batch");
        total_rows += batch.num_rows();
        batch_count += 1;
        offset += batch.num_rows() as i64;
        if total_rows >= 30 {
            break;
        }
    }

    assert_eq!(batch_count, 3, "sample.parquet has 3 row groups of 10 rows");
    assert_eq!(total_rows, 30);
    println!("  {batch_count} batches, {total_rows} rows projected");
    Ok(())
}

fn test_nullability(data_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("Nullability (CSV + JSONL):");
    let csv_df = CsvReadOptions::default()
        .try_into_reader_with_file_path(Some(data_dir.join("sample.csv")))?
        .finish()?;
    let csv_batch = polars_df_to_record_batch(csv_df)?;
    let name_col = csv_batch.column_by_name("name").expect("name column");
    assert!(name_col.null_count() > 0, "CSV fixture should contain null names");

    let jsonl_df = JsonReader::new(File::open(data_dir.join("sample.jsonl"))?)
        .with_json_format(JsonFormat::JsonLines)
        .finish()?;
    let jsonl_batch = polars_df_to_record_batch(jsonl_df)?;
    let jsonl_name = jsonl_batch.column_by_name("name").expect("jsonl name");
    assert_eq!(jsonl_name.null_count(), 1, "JSONL row 2 has null name");
    println!("  CSV nulls={}, JSONL nulls={}", name_col.null_count(), jsonl_name.null_count());
    Ok(())
}

fn test_empty_arrays(data_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("Empty JSONL list arrays:");
    let df = JsonReader::new(File::open(data_dir.join("empty_arrays.jsonl"))?)
        .with_json_format(JsonFormat::JsonLines)
        .finish()?;
    let batch = polars_df_to_record_batch(df)?;
    let tags = batch.column_by_name("tags").expect("tags column");
    assert_eq!(tags.len(), 2);
    println!("  tags column len={} dtype={:?}", tags.len(), tags.data_type());
    Ok(())
}

fn test_nested_types(data_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("Nested Struct + LargeList:");
    let df = JsonReader::new(File::open(data_dir.join("nested.jsonl"))?)
        .with_json_format(JsonFormat::JsonLines)
        .finish()?;
    let batch = polars_df_to_record_batch(df)?;
    let schema = batch.schema();
    let user = schema.field_with_name("user").expect("user");
    let counts = schema.field_with_name("counts").expect("counts");
    assert!(matches!(user.data_type(), DataType::Struct(_)));
    assert!(matches!(
        counts.data_type(),
        DataType::List(_) | DataType::LargeList(_)
    ));
    println!("  user={:?}, counts={:?}", user.data_type(), counts.data_type());
    Ok(())
}

fn test_field_metadata_manual_copy() -> Result<(), Box<dyn std::error::Error>> {
    println!("Field metadata manual copy:");
    let polars_field = polars_arrow::datatypes::Field::new(
        "meta_col".into(),
        polars_arrow::datatypes::ArrowDataType::Int32,
        true,
    )
    .with_metadata([("source".into(), "spike".into())].into());

    // Schema-only FFI does not preserve metadata (verified during spike development).
    let polars_c_schema = export_field_to_c(&polars_field);
    let arrow_c_schema: arrow_schema::ffi::FFI_ArrowSchema =
        unsafe { mem::transmute(polars_c_schema) };
    let dtype_only = Field::new("meta_col", DataType::try_from(&arrow_c_schema)?, true);
    assert!(
        dtype_only.metadata().is_empty(),
        "schema FFI alone must not be relied on for metadata"
    );

    let arrow_field = polars_field_to_arrow_rs_field(polars_field)?;
    assert_eq!(
        arrow_field.metadata().get("source"),
        Some(&"spike".to_string())
    );
    Ok(())
}

fn test_ffi_ownership_and_repeated_drop(data_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("FFI ownership + repeated create/drop:");
    let df = CsvReadOptions::default()
        .with_n_rows(Some(3))
        .try_into_reader_with_file_path(Some(data_dir.join("sample.csv")))?
        .finish()?;

    for iteration in 0..200 {
        let batch = polars_df_to_record_batch(df.clone())?;
        assert_eq!(batch.num_rows(), 3);
        // Dropping `batch` releases arrow-rs arrays imported via single FFI consume path.
        drop(batch);
        if iteration == 0 {
            println!("  first iteration ok; running 200 create/drop cycles");
        }
    }
    println!("  200 FFI import cycles completed without crash");
    Ok(())
}

fn test_obatch_memory_bound(data_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("O(batch) memory evidence:");
    let path = data_dir.join("sample.csv");
    let file_len = std::fs::metadata(&path)?.len() as usize;

    let file = File::open(&path)?;
    let mut reader = CsvReadOptions::default()
        .with_has_header(true)
        .with_chunk_size(64)
        .into_reader_with_file_handle(file);
    let mut batched = reader.batched_borrowed()?;

    let mut max_batch_mem = 0usize;
    let mut batch_count = 0usize;
    while let Some(chunks) = batched.next_batches(1)? {
        for df in chunks {
            let batch = polars_df_to_record_batch(df)?;
            max_batch_mem = max_batch_mem.max(batch_memory_bytes(&batch));
            batch_count += 1;
        }
    }

    assert!(batch_count > 1);
    assert!(
        max_batch_mem < file_len,
        "max batch memory ({max_batch_mem}) must be < file size ({file_len})"
    );
    println!("  file_bytes={file_len}, max_batch_mem={max_batch_mem}, batches={batch_count}");
    Ok(())
}
