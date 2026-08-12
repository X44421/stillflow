use std::collections::BTreeMap;
use std::io::BufRead;

use serde::de::IgnoredAny;
use serde::Deserialize;
use serde_json::{Map, Value};
use stillflow_core::{
    ConnectorError, ConnectorResult, ErrorCategory, RequestContext, MAX_BATCH_BYTES,
};

use crate::format::TabularFormat;

const UTF8_BOM: &[u8] = b"\xEF\xBB\xBF";

pub(crate) struct JsonObjectStream<R> {
    reader: R,
    shape: JsonShape,
    initialized: bool,
    finished: bool,
    after_comma: bool,
    row: usize,
}

#[derive(Debug, Clone, Copy)]
enum JsonShape {
    Array,
    Lines,
}

impl<R: BufRead> JsonObjectStream<R> {
    pub(crate) fn new(reader: R, format: TabularFormat) -> ConnectorResult<Self> {
        let shape = match format {
            TabularFormat::Json => JsonShape::Array,
            TabularFormat::Ndjson => JsonShape::Lines,
            _ => {
                return Err(ConnectorError::invalid_configuration(
                    "JSON object stream requires JSON or NDJSON format",
                ));
            }
        };
        Ok(Self {
            reader,
            shape,
            initialized: false,
            finished: false,
            after_comma: false,
            row: 0,
        })
    }

    pub(crate) fn next_object(
        &mut self,
        context: &RequestContext,
    ) -> ConnectorResult<Option<Map<String, Value>>> {
        let Some(raw) = self.next_raw_object(context)? else {
            return Ok(None);
        };
        parse_object(&raw, self.row).map(Some)
    }

    pub(crate) fn next_raw_object(
        &mut self,
        context: &RequestContext,
    ) -> ConnectorResult<Option<Vec<u8>>> {
        context.ensure_active()?;
        if self.finished {
            return Ok(None);
        }
        match self.shape {
            JsonShape::Array => self.next_array_object(context),
            JsonShape::Lines => self.next_line_object(context),
        }
    }

    pub(crate) const fn row_number(&self) -> usize {
        self.row
    }

    pub(crate) fn sample_is_truncated(&mut self) -> ConnectorResult<bool> {
        if self.finished {
            return Ok(false);
        }
        match self.shape {
            JsonShape::Array => Ok(true),
            JsonShape::Lines => {
                self.skip_whitespace()?;
                Ok(self.peek_byte()?.is_some())
            }
        }
    }

    fn next_array_object(&mut self, context: &RequestContext) -> ConnectorResult<Option<Vec<u8>>> {
        if !self.initialized {
            self.consume_bom()?;
            self.skip_whitespace()?;
            match self.read_byte()? {
                Some(b'[') => {}
                None => return Err(json_error("JSON source ended before its top-level array")),
                Some(_) => return Err(json_error("JSON source must be one top-level array")),
            }
            self.initialized = true;
        }

        self.skip_whitespace()?;
        if self.peek_byte()? == Some(b']') {
            if self.after_comma {
                return Err(json_error("JSON array must not contain a trailing comma"));
            }
            self.read_byte()?;
            self.finish_array()?;
            return Ok(None);
        }
        if self.peek_byte()?.is_none() {
            return Err(json_error("JSON array ended before its closing bracket"));
        }

        let raw = self.read_balanced_object(context)?;
        let next_row = self
            .row
            .checked_add(1)
            .ok_or_else(|| json_error("JSON row count exceeds the supported range"))?;
        validate_object_syntax(&raw, next_row)?;
        self.after_comma = false;
        self.row = next_row;
        self.skip_whitespace()?;
        match self.read_byte()? {
            Some(b',') => self.after_comma = true,
            Some(b']') => self.finish_array()?,
            None => {
                return Err(json_error(
                    "JSON array ended before its separator or closing bracket",
                ));
            }
            Some(_) => {
                return Err(json_error(
                    "JSON array elements must be separated by commas",
                ));
            }
        }
        Ok(Some(raw))
    }

    fn next_line_object(&mut self, context: &RequestContext) -> ConnectorResult<Option<Vec<u8>>> {
        if !self.initialized {
            self.consume_bom()?;
            self.initialized = true;
        }
        loop {
            context.ensure_active()?;
            let mut line = Vec::new();
            loop {
                context.ensure_active()?;
                let available = self
                    .reader
                    .fill_buf()
                    .map_err(|_| json_io_error("an NDJSON line could not be read", true))?;
                if available.is_empty() {
                    break;
                }
                let consumed = available
                    .iter()
                    .position(|byte| *byte == b'\n')
                    .map_or(available.len(), |index| index + 1);
                let ended = available.get(consumed - 1) == Some(&b'\n');
                let decoded = available.get(..consumed).ok_or_else(|| {
                    json_io_error("the NDJSON decoder exceeded its input buffer", false)
                })?;
                ensure_object_bytes(line.len(), decoded.len())?;
                line.extend_from_slice(decoded);
                self.reader.consume(consumed);
                if ended {
                    break;
                }
            }
            if line.is_empty() {
                self.finished = true;
                return Ok(None);
            }
            if line.last() == Some(&b'\n') {
                line.pop();
            }
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            self.row = self
                .row
                .checked_add(1)
                .ok_or_else(|| json_error("NDJSON row count exceeds the supported range"))?;
            return Ok(Some(line));
        }
    }

    fn read_balanced_object(&mut self, context: &RequestContext) -> ConnectorResult<Vec<u8>> {
        if self.read_byte()? != Some(b'{') {
            return Err(json_error("every JSON array element must be an object"));
        }
        let mut raw = vec![b'{'];
        let mut depth = 1_usize;
        let mut in_string = false;
        let mut escaped = false;
        while depth > 0 {
            if raw.len() % 4096 == 0 {
                context.ensure_active()?;
            }
            let Some(byte) = self.read_byte()? else {
                return Err(json_error("JSON object ended before its closing brace"));
            };
            ensure_object_bytes(raw.len(), 1)?;
            raw.push(byte);
            if in_string {
                if escaped {
                    escaped = false;
                } else if byte == b'\\' {
                    escaped = true;
                } else if byte == b'"' {
                    in_string = false;
                }
                continue;
            }
            match byte {
                b'"' => in_string = true,
                b'{' | b'[' => {
                    depth = depth.checked_add(1).ok_or_else(|| {
                        json_error("JSON nesting exceeds the supported parser range")
                    })?
                }
                b'}' | b']' => {
                    depth = depth.checked_sub(1).ok_or_else(|| {
                        json_error("JSON contains an unmatched closing delimiter")
                    })?;
                }
                _ => {}
            }
        }
        Ok(raw)
    }

    fn finish_array(&mut self) -> ConnectorResult<()> {
        self.skip_whitespace()?;
        if self.peek_byte()?.is_some() {
            return Err(json_error("JSON contains data after the top-level array"));
        }
        self.finished = true;
        Ok(())
    }

    fn consume_bom(&mut self) -> ConnectorResult<()> {
        let available = self
            .reader
            .fill_buf()
            .map_err(|_| json_io_error("JSON input could not be buffered", true))?;
        if available.starts_with(UTF8_BOM) {
            self.reader.consume(UTF8_BOM.len());
        }
        Ok(())
    }

    fn skip_whitespace(&mut self) -> ConnectorResult<()> {
        loop {
            let available = self
                .reader
                .fill_buf()
                .map_err(|_| json_io_error("JSON input could not be buffered", true))?;
            let count = available
                .iter()
                .take_while(|byte| byte.is_ascii_whitespace())
                .count();
            let empty = available.is_empty();
            self.reader.consume(count);
            if empty || count == 0 {
                return Ok(());
            }
        }
    }

    fn peek_byte(&mut self) -> ConnectorResult<Option<u8>> {
        self.reader
            .fill_buf()
            .map(|bytes| bytes.first().copied())
            .map_err(|_| json_io_error("JSON input could not be buffered", true))
    }

    fn read_byte(&mut self) -> ConnectorResult<Option<u8>> {
        let value = self.peek_byte()?;
        if value.is_some() {
            self.reader.consume(1);
        }
        Ok(value)
    }
}

fn ensure_object_bytes(current: usize, additional: usize) -> ConnectorResult<()> {
    let total = current
        .checked_add(additional)
        .ok_or_else(|| json_error("JSON row exceeds the supported decoded byte range"))?;
    if total > MAX_BATCH_BYTES {
        return Err(json_error("JSON row exceeds the public batch byte bound"));
    }
    Ok(())
}

fn parse_object(bytes: &[u8], row: usize) -> ConnectorResult<Map<String, Value>> {
    let value: Value = serde_json::from_slice(bytes)
        .map_err(|_| json_error_at_row("JSON object is malformed", row))?;
    match value {
        Value::Object(object) => Ok(object),
        _ => Err(json_error_at_row("JSON row is not an object", row)),
    }
}

fn validate_object_syntax(bytes: &[u8], row: usize) -> ConnectorResult<()> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    IgnoredAny::deserialize(&mut deserializer)
        .map_err(|_| json_error_at_row("JSON object is malformed", row))?;
    deserializer
        .end()
        .map_err(|_| json_error_at_row("JSON object is malformed", row))
}

fn json_error(message: &'static str) -> ConnectorError {
    json_io_error(message, false)
}

fn json_error_at_row(message: &'static str, row: usize) -> ConnectorError {
    ConnectorError::with_category(
        ErrorCategory::InvalidData,
        false,
        format!("{message} at row {row}"),
        Vec::new(),
        BTreeMap::new(),
    )
}

fn json_io_error(message: &'static str, retryable: bool) -> ConnectorError {
    ConnectorError::with_category(
        if retryable {
            ErrorCategory::TransientSource
        } else {
            ErrorCategory::InvalidData
        },
        retryable,
        message,
        Vec::new(),
        BTreeMap::new(),
    )
}

#[cfg(test)]
mod tests {
    use std::io::{BufReader, Cursor};

    use super::*;

    #[test]
    fn streams_nested_array_objects_without_consuming_following_rows() {
        let input = br#"[{"a":{"nested":[1,{"x":"}"}]}},{"a":null}]"#;
        let mut stream =
            JsonObjectStream::new(BufReader::new(Cursor::new(input)), TabularFormat::Json)
                .expect("stream");
        let context = RequestContext::default();
        assert!(stream.next_object(&context).expect("first").is_some());
        assert!(stream.next_object(&context).expect("second").is_some());
        assert!(stream.next_object(&context).expect("end").is_none());
    }

    #[test]
    fn rejects_non_object_rows_and_trailing_data() {
        for input in [br#"[1]"#.as_slice(), br#"[{}] false"#.as_slice()] {
            let mut stream =
                JsonObjectStream::new(BufReader::new(Cursor::new(input)), TabularFormat::Json)
                    .expect("stream");
            assert!(stream.next_object(&RequestContext::default()).is_err());
        }
    }
}
