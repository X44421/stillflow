//! Bounded CSV decoding over the Polars 0.55 reader primitives (P55-A1).
//!
//! Polars 0.46 exposed `CsvReader::batched` / `OwnedBatchedCsvReader` and the
//! connector used it to decode CSV in bounded row windows. Polars 0.55 removed
//! that reader; `CsvReadOptions::into_reader_with_file_handle` now yields a
//! `CsvReader` whose only public terminals are whole-file `read`/`finish`.
//!
//! Materialising a whole file would break the connector's bounded-memory
//! contract, so this adapter rebuilds the batched capability on the pieces
//! Polars still exports for that purpose:
//!
//! - `ReaderBytes` memory-maps a file-backed reader through Polars' own mmap
//!   semaphore, so the file never lands in an owned buffer;
//! - `SplitLines` finds quote-aware line boundaries, so each window ends on a
//!   record boundary;
//! - `read_chunk` is the same parser the removed batched reader called, so the
//!   decoded values keep their 0.46 semantics;
//! - `prepare_csv_schema` and `cast_columns` keep the "parse natively, then
//!   cast to the requested dtype" split that `CsvReader` uses internally.
//!
//! Only one window of rows is decoded at a time, and the row budget from
//! `with_n_rows` is enforced on the decoded frames, so callers keep observing
//! the same batch heights, the same end-of-file signal and the same
//! `PolarsError` failure classes as before.

use polars::error::PolarsError;
use polars::io::csv::read::_csv_read_internal::{
    cast_columns, prepare_csv_schema, read_chunk, NullValuesCompiled,
};
use polars::io::csv::read::{CsvReadOptions, SplitLines};
use polars::io::mmap::{MmapBytesReader, ReaderBytes};
use polars::prelude::{CsvParseOptions, DataFrame, Field, SchemaRef};

/// Decodes a CSV file in bounded row windows.
pub(crate) struct BoundedCsvDecoder {
    bytes: ReaderBytes<'static>,
    parse_options: CsvParseOptions,
    /// The parse schema after `prepare_csv_schema` coercion.
    schema: SchemaRef,
    /// Fields whose parsed dtype must be cast afterwards.
    to_cast: Vec<Field>,
    /// Sorted projection into `schema`.
    projection: Vec<usize>,
    null_values: Option<NullValuesCompiled>,
    ignore_errors: bool,
    /// Whether the first line is a header and must be skipped.
    has_header: bool,
    /// Rows decoded per window.
    chunk_rows: usize,
    /// Remaining row budget from `with_n_rows`.
    remaining: usize,
    /// Absolute offset of the next undecoded byte.
    offset: usize,
    /// Whether the header line has already been skipped.
    started: bool,
}

impl BoundedCsvDecoder {
    /// Prepares a decoder from the same options the reader path already builds.
    ///
    /// The reader must be file-backed: the adapter keeps Polars' mmap alive for
    /// the lifetime of the decoder instead of copying the file into memory.
    pub(crate) fn new(
        options: &CsvReadOptions,
        mut reader: Box<dyn MmapBytesReader>,
    ) -> Result<Self, PolarsError> {
        let reader_bytes = ReaderBytes::from(&mut reader);
        let bytes = match reader_bytes {
            ReaderBytes::Owned(buffer) => ReaderBytes::Owned(buffer),
            ReaderBytes::Borrowed(_) => {
                return Err(PolarsError::ComputeError(
                    "the bounded CSV decoder needs a file-backed reader".into(),
                ))
            }
        };

        if options.skip_rows != 0
            || options.skip_lines != 0
            || options.skip_rows_after_header != 0
            || options.row_index.is_some()
            || options.columns.is_some()
            || options.schema_overwrite.is_some()
        {
            return Err(PolarsError::ComputeError(
                "the bounded CSV decoder does not implement this read option".into(),
            ));
        }

        let mut schema = options.schema.clone().ok_or_else(|| {
            PolarsError::ComputeError("the bounded CSV decoder needs an explicit schema".into())
        })?;
        let mut to_cast = options.fields_to_cast.clone();
        prepare_csv_schema(&mut schema, &mut to_cast)?;

        let parse_options = (*options.parse_options).clone();
        let null_values = parse_options
            .null_values
            .as_ref()
            .map(|values| values.clone().compile(&schema))
            .transpose()?;

        // `read_chunk` requires a sorted projection, and it indexes the full
        // parse schema, not the projected subset.
        let mut projection = match &options.projection {
            Some(projection) => projection.as_ref().clone(),
            None => (0..schema.len()).collect(),
        };
        if let Some(index) = projection.iter().max() {
            if *index >= schema.len() {
                return Err(PolarsError::ComputeError(
                    "the CSV projection index is out of bounds for the parse schema".into(),
                ));
            }
        }
        projection.sort_unstable();

        Ok(Self {
            bytes,
            parse_options,
            schema,
            to_cast,
            projection,
            null_values,
            ignore_errors: options.ignore_errors,
            has_header: options.has_header,
            chunk_rows: options.chunk_size.max(1),
            remaining: options.n_rows.unwrap_or(usize::MAX),
            offset: 0,
            started: false,
        })
    }

    /// Decodes the next bounded row batch.
    ///
    /// Returns `None` once the file is exhausted or the row budget is spent,
    /// matching the removed `OwnedBatchedCsvReader::next_batches` signal.
    pub(crate) fn next_batches(
        &mut self,
        count: usize,
    ) -> Result<Option<Vec<DataFrame>>, PolarsError> {
        if count == 0 || self.remaining == 0 {
            return Ok(None);
        }

        let mut batch = Vec::with_capacity(count);
        for _ in 0..count {
            match self.next_frame()? {
                Some(frame) => batch.push(frame),
                None => break,
            }
        }
        if batch.is_empty() {
            return Ok(None);
        }
        Ok(Some(batch))
    }

    fn next_frame(&mut self) -> Result<Option<DataFrame>, PolarsError> {
        let rest = self.bytes.get(self.offset..).ok_or_else(|| {
            PolarsError::ComputeError("the bounded CSV decoder lost its byte window".into())
        })?;

        if !self.started {
            self.started = true;
            if self.has_header {
                self.offset = match SplitLines::new(
                    rest,
                    self.parse_options.quote_char,
                    self.parse_options.eol_char,
                    None,
                )
                .next()
                {
                    Some(header) => self.offset + self.line_length(rest, header),
                    None => self.bytes.len(),
                };
            }
        }

        let rest = self.bytes.get(self.offset..).ok_or_else(|| {
            PolarsError::ComputeError("the bounded CSV decoder lost its byte window".into())
        })?;
        let capacity = self.chunk_rows.min(self.remaining);
        let mut window_end = self.offset;
        let mut lines = 0_usize;
        for line in SplitLines::new(
            rest,
            self.parse_options.quote_char,
            self.parse_options.eol_char,
            None,
        ) {
            if lines == capacity {
                break;
            }
            lines += 1;
            window_end = self.offset + self.line_length(rest, line);
        }
        if lines == 0 {
            return Ok(None);
        }

        let window = self.bytes.get(self.offset..window_end).ok_or_else(|| {
            PolarsError::ComputeError("the bounded CSV decoder lost its byte window".into())
        })?;
        let mut frame = read_chunk(
            window,
            &self.parse_options,
            &self.schema,
            self.ignore_errors,
            &self.projection,
            0,
            capacity,
            self.null_values.as_ref(),
            usize::MAX,
            window.len(),
            // `read_chunk` adds this to the offsets it reports, so the window
            // start keeps those offsets absolute.
            Some(self.offset),
        )?;
        cast_columns(&mut frame, &self.to_cast, false, self.ignore_errors)?;

        self.offset = window_end;
        if frame.height() > self.remaining {
            frame = frame.slice(0, self.remaining);
        }
        self.remaining = self.remaining.saturating_sub(frame.height());
        Ok(Some(frame))
    }

    /// The number of bytes `line` occupies inside `buffer`, including the
    /// end-of-line byte when one follows it.
    fn line_length(&self, buffer: &[u8], line: &[u8]) -> usize {
        let base = buffer.as_ptr() as usize;
        let end = (line.as_ptr() as usize).saturating_sub(base) + line.len();
        match buffer.get(end) {
            Some(byte) if *byte == self.parse_options.eol_char => end + 1,
            _ => end,
        }
    }
}
