//! E24-JSON-A2 — direct projected NDJSON writer (private feature
//! `json-direct-projected-writer`, issues #148/#158).
//!
//! Replaces ONLY the selected-field generic `serde_json::Value` tree plus
//! projected `Map<String, Value>` reconstruction of the projected NDJSON path
//! (`read.rs` `parse_projected_object`). Everything else is shared with the
//! generic path and unchanged:
//!
//! * non-selected fields keep the typed `ValidateFieldSeed` streaming path;
//! * selected values are captured as borrowed raw JSON slices
//!   (`serde_json::value::RawValue`) instead of a generic DOM and validated
//!   immediately through the SAME shared typed visitor
//!   (`read::LogicalValueVisitor` via `read::ValidateFieldSeed`) at the SAME
//!   observable point as the generic path (blocking before advancing past the
//!   current object key), so the earliest failing key/value position and the
//!   error-category ordering are unchanged;
//! * top-level duplicate detection stays at the same point, BEFORE value
//!   acceptance;
//! * the projected row is assembled deterministically in `projection.schema`
//!   order by writing precomputed key syntax plus already-valid raw value
//!   bytes;
//! * the downstream Polars `JsonReader` second parse is intentionally
//!   retained; no Arrow/DataFrame builder is introduced and no public API or
//!   `LogicalSchema` surface changes.
//!
//! # Bounded-memory shape
//!
//! * no generic selected `Value` tree and no projected `Map<String, Value>`
//!   exists anywhere on this path;
//! * `ProjectedRowAssembler` owns per-batch scratch only: escaped key prefixes
//!   (O(projected fields x key length)), the projected-name slot index, and the
//!   per-row slot states. It is constructed per `fill_pending` frame and
//!   dropped with it, so nothing lives longer than one batch;
//! * per row, each captured selected value is recorded either as a borrowed
//!   byte span pointing INTO the framed row buffer that the generic path also
//!   allocates (`next_raw_object`), or — for the one disclosed canonicalization
//!   fallback below — as an owned copy bounded by that subtree's own bytes.
//!   Slot states are reset at the start of every row, so no slot outlives a
//!   row;
//! * the assembled row written into the shared batch `Vec<u8>` (`encoded` in
//!   `read.rs`) is the single row-sized owned buffer, identical to the generic
//!   path's re-encode target; no selected value is copied through any
//!   intermediate owned buffer between capture and final assembly;
//! * canonicalization fallback (bounded by the captured subtree's own bytes,
//!   dropped at the end of the row): a selected value is re-encoded once,
//!   exactly like the generic path's `Value` re-serialization, when
//!   (a) it contained duplicate nested keys — `serde_json::Value` with
//!   `preserve_order` collapses those last-wins at the original position, and
//!   a raw slice would hand every occurrence downstream — (b) it contains a
//!   raw JSON control byte (anything below 0x20) or an integer literal wider
//!   than serde_json's own integer parse, both of which only the generic
//!   DOM re-encoding normalizes the same way, or (c) its streaming validation
//!   failed but the value is a List/Struct, where the collapsed DOM form —
//!   the exact form the generic path validates — decides acceptance. Strings
//!   cannot contain raw control bytes (the capture scan rejects them), so raw
//!   control bytes only ever occur inside List/Struct captures: the generic
//!   path re-serializes compactly, while a raw newline (or carriage return)
//!   inside an assembled row would split the row inside the downstream
//!   line-oriented `JsonReader`.
//!
//! # Duplicate-key validation parity
//!
//! The generic path validates each selected value AFTER its DOM parse, so a
//! nested object that repeats a key is validated in its collapsed (last value
//! wins) form only. The streaming second parse below validates every
//! occurrence instead, which would REJECT a subtree whose non-last occurrence
//! is itself invalid (e.g. `{"x":"bad","x":2}` for `Struct{x: Int64}`) even
//! though the generic path accepts it. Whenever a captured List/Struct value
//! fails the streaming validation, the exact DOM form is therefore rebuilt and
//! pushed through the generic oracle (`read::validate_json_value`); its
//! verdict is authoritative and reproduces the generic accept/reject surface
//! mechanically. If the DOM itself cannot be built (parse-class failures such
//! as number ranges), the streaming error's classification already matches the
//! generic deserializer-level surface (see `syntax_error`).
//! * known measured trade-off (disclosed, not hidden): the captured raw slice
//!   is scanned twice — once syntactically during capture, once by the typed
//!   validator — but never copied; the generic path scans once and allocates a
//!   full DOM copy instead.
//!
//! # Error-surface parity notes
//!
//! The generic path surfaces DOM-parse failures of selected values (number
//! ranges, unicode escapes) as syntax-classified errors, while a raw slice
//! capture scan does not range-check numbers. `validate_selected_raw_value`
//! re-parses the slice through the shared typed visitor and records the
//! syntax/error classification so `encode_row` can reproduce the exact generic
//! category mapping. Map keys deserialize through `Cow<str>` so escaped keys
//! are accepted and deduplicated exactly like the generic path's `String`
//! keys (a plain `&str` key would reject every escaped key).

use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap};

use serde::de::{DeserializeSeed, Error as DeError, MapAccess, Visitor};
use serde_json::value::RawValue;
use stillflow_core::{ConnectorResult, ErrorCategory, LogicalField, LogicalSchema, LogicalType};

use crate::read::{source_error, source_error_with_row, validate_json_value, ValidateFieldSeed};

/// One projected field's captured value for the row currently being encoded.
enum ProjectedSlot {
    /// No value captured: the field is absent from the input row and the
    /// assembled row carries an explicit `null`, like the generic path.
    Empty,
    /// Borrowed byte span into the current framed row (`raw`). Valid only
    /// within the `encode_row` call that produced it; slot states are reset
    /// before every row, so no span outlives its framed row buffer.
    Raw { start: usize, end: usize },
    /// Canonicalized owned copy (generic `Value` semantics: nested duplicate
    /// keys collapsed last-wins, whitespace normalized). Bounded by the
    /// captured subtree's own bytes and dropped with the row.
    Canonical(Vec<u8>),
}

/// Per-batch scratch for the direct projected writer. Constructed per
/// `fill_pending` frame and dropped with it.
pub(crate) struct ProjectedRowAssembler {
    key_prefixes: Vec<Vec<u8>>,
    slot_of: HashMap<String, usize>,
    slots: Vec<ProjectedSlot>,
}

impl ProjectedRowAssembler {
    /// Precomputes the `"escaped_key":` prefix for every projected field so
    /// per-row assembly only concatenates bytes, and the projected-name slot
    /// index. Bounded by O(projected fields) per batch frame.
    pub(crate) fn new(names: &[String]) -> ConnectorResult<Self> {
        let key_prefixes = names
            .iter()
            .map(|name| {
                let mut prefix = serde_json::to_vec(name).map_err(|_| {
                    source_error(
                        ErrorCategory::Internal,
                        false,
                        "projected JSON key could not be encoded",
                    )
                })?;
                prefix.push(b':');
                Ok(prefix)
            })
            .collect::<ConnectorResult<Vec<_>>>()?;
        let slot_of = names
            .iter()
            .enumerate()
            .map(|(index, name)| (name.clone(), index))
            .collect();
        Ok(Self {
            key_prefixes,
            slot_of,
            slots: (0..names.len()).map(|_| ProjectedSlot::Empty).collect(),
        })
    }

    /// Validates one framed input row and appends its projected NDJSON
    /// encoding, in `projection.schema` order, to `out` (without the trailing
    /// newline).
    pub(crate) fn encode_row(
        &mut self,
        raw: &[u8],
        schema: &LogicalSchema,
        row: usize,
        out: &mut Vec<u8>,
    ) -> ConnectorResult<()> {
        if raw.iter().copied().find(|byte| !byte.is_ascii_whitespace()) != Some(b'{') {
            return Err(source_error_with_row(
                ErrorCategory::InvalidData,
                "JSON row is not an object",
                row,
            ));
        }
        for slot in &mut self.slots {
            *slot = ProjectedSlot::Empty;
        }
        let mut deserializer = serde_json::Deserializer::from_slice(raw);
        let mut syntax_error = false;
        DirectProjectedObjectSeed {
            schema,
            slot_of: &self.slot_of,
            slots: &mut self.slots,
            base: raw.as_ptr() as usize,
            syntax_error: &mut syntax_error,
        }
        .deserialize(&mut deserializer)
        .map_err(|error| {
            let category = if syntax_error {
                // A selected value failed the generic path's DOM-parse surface
                // (number range, unicode escape): reproduce the syntax mapping.
                ErrorCategory::InvalidData
            } else {
                match error.classify() {
                    serde_json::error::Category::Syntax | serde_json::error::Category::Eof => {
                        ErrorCategory::InvalidData
                    }
                    serde_json::error::Category::Io => ErrorCategory::TransientSource,
                    serde_json::error::Category::Data => ErrorCategory::SchemaDrift,
                }
            };
            source_error_with_row(
                category,
                "JSON row does not match the established schema",
                row,
            )
        })?;
        deserializer.end().map_err(|_| {
            source_error_with_row(ErrorCategory::InvalidData, "JSON row is malformed", row)
        })?;

        out.push(b'{');
        for (index, prefix) in self.key_prefixes.iter().enumerate() {
            if index > 0 {
                out.push(b',');
            }
            out.extend_from_slice(prefix);
            match self.slots.get(index) {
                Some(ProjectedSlot::Raw { start, end }) => {
                    let bytes = raw.get(*start..*end).ok_or_else(|| {
                        source_error(
                            ErrorCategory::Internal,
                            false,
                            "captured projected value escaped its row frame",
                        )
                    })?;
                    out.extend_from_slice(bytes);
                }
                Some(ProjectedSlot::Canonical(bytes)) => out.extend_from_slice(bytes),
                _ => out.extend_from_slice(b"null"),
            }
        }
        out.push(b'}');
        Ok(())
    }
}

struct DirectProjectedObjectSeed<'a, 's> {
    schema: &'a LogicalSchema,
    slot_of: &'a HashMap<String, usize>,
    slots: &'s mut Vec<ProjectedSlot>,
    /// Start address of the framed row; captured raw slices are sub-slices of
    /// that row (`serde_json` `SliceRead` raw-value capture borrows the input),
    /// so their spans are recorded as address offsets and re-checked at
    /// assembly time.
    base: usize,
    syntax_error: &'s mut bool,
}

impl<'de> DeserializeSeed<'de> for DirectProjectedObjectSeed<'_, '_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(DirectProjectedObjectVisitor {
            schema: self.schema,
            slot_of: self.slot_of,
            slots: self.slots,
            base: self.base,
            syntax_error: self.syntax_error,
        })
    }
}

struct DirectProjectedObjectVisitor<'a, 's> {
    schema: &'a LogicalSchema,
    slot_of: &'a HashMap<String, usize>,
    slots: &'s mut Vec<ProjectedSlot>,
    base: usize,
    syntax_error: &'s mut bool,
}

impl<'de> Visitor<'de> for DirectProjectedObjectVisitor<'_, '_> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON object matching the established schema")
    }

    fn visit_map<A>(self, mut access: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        // Borrowed keys never allocate; escaped keys copy through serde's
        // scratch as `Cow::Owned`, so acceptance and duplicate detection match
        // the generic path's `String` keys exactly.
        let mut seen = BTreeSet::new();
        while let Some(name) = access.next_key::<Cow<'de, str>>()? {
            let Some(field) = self.schema.fields.iter().find(|field| field.name == name) else {
                return Err(A::Error::custom(
                    "JSON row contains a field outside the established schema",
                ));
            };
            if !seen.insert(name.clone()) {
                return Err(A::Error::custom("JSON row contains a duplicate field"));
            }
            let Some(&slot) = self.slot_of.get(name.as_ref()) else {
                // Non-selected: the exact generic typed streaming path.
                access.next_value_seed(ValidateFieldSeed {
                    field,
                    duplicates: None,
                })?;
                continue;
            };
            let target = self
                .slots
                .get_mut(slot)
                .ok_or_else(|| A::Error::custom("captured projected slot is out of range"))?;
            // Same observable point as the generic path: capture the selected
            // value as raw JSON and validate it through the shared typed
            // machinery BEFORE advancing to the next key.
            let captured: &RawValue = access.next_value()?;
            let subtree = matches!(
                field.data_type,
                LogicalType::List(_) | LogicalType::Struct(_)
            );
            let mut canonicalize = false;
            match validate_selected_raw_value(
                captured.get(),
                field,
                &mut canonicalize,
                &mut *self.syntax_error,
            ) {
                Ok(()) => {
                    // Generic selected values pass through `serde_json::Value`,
                    // which re-serializes compactly. Three inputs need that
                    // same canonicalization here (see the module docs for the
                    // mechanics): nested duplicate keys, any raw JSON control
                    // byte inside the subtree, and integer literals too wide
                    // for serde_json's own integer parse (the generic path
                    // re-encodes those as floats, and the retained Polars
                    // JsonReader rejects the raw integer form on float
                    // columns).
                    let needs_canonicalization = canonicalize
                        || (subtree && captured.get().as_bytes().iter().any(|&byte| byte < 0x20))
                        || (type_can_contain_number_tokens(&field.data_type)
                            && has_wide_integer_literal(captured.get()))
                        || contains_timestamp(&field.data_type);
                    if needs_canonicalization {
                        *target = canonical_captured_value(captured.get(), field)
                            .map_err(A::Error::custom)?;
                    } else {
                        record_raw_span(captured, self.base, target).map_err(A::Error::custom)?;
                    }
                }
                Err(message) => {
                    // The generic path validates the DOM-collapsed (last value
                    // wins) form of every selected value, so a subtree whose
                    // non-last repeated occurrence is invalid is accepted there.
                    // Whenever the capture can contain repeated keys, rebuild
                    // the exact DOM form and let the generic oracle decide.
                    let mut recovered = None;
                    if subtree {
                        if let Ok(dom) = serde_json::from_str::<serde_json::Value>(captured.get()) {
                            if validate_json_value(&dom, field).is_ok() {
                                recovered = Some(serde_json::to_vec(&dom).map_err(|_| {
                                    A::Error::custom("value has an incompatible logical type")
                                })?);
                            }
                            // A rejected DOM form is exactly the generic
                            // rejection; fall through to the error below.
                        }
                        // A DOM build failure is a parse-class surface the
                        // streaming error already classified identically to the
                        // generic deserializer (number ranges, depth limits).
                    }
                    match recovered {
                        Some(bytes) => *target = ProjectedSlot::Canonical(bytes),
                        None => return Err(A::Error::custom(message)),
                    }
                }
            }
        }
        for field in &self.schema.fields {
            if !seen.contains(field.name.as_str()) && !field.nullable {
                return Err(A::Error::custom("JSON row is missing a required field"));
            }
        }
        Ok(())
    }
}

/// Validates a captured raw slice through the shared typed obligations.
///
/// The slice is already known to be syntactically well-formed JSON (capture
/// scans it), so every reachable failure is either semantic (custom/Data —
/// the same surface the generic path produced from its typed DOM validation)
/// or one of the parse classes the capture scan deliberately does not
/// range-check: number ranges and unicode escape validity. Those map to the
/// generic path's syntax-classified DOM-parse surface through the
/// `syntax_error` flag instead of degrading to the semantic SchemaDrift
/// surface. The `canonicalize` flag records whether any nested object
/// repeated a key, so the caller can collapse the value to generic-path
/// `Value` semantics (last value wins at the original position).
fn validate_selected_raw_value(
    text: &str,
    field: &LogicalField,
    canonicalize: &mut bool,
    syntax_error: &mut bool,
) -> Result<(), &'static str> {
    let mut deserializer = serde_json::Deserializer::from_slice(text.as_bytes());
    let result = ValidateFieldSeed {
        field,
        duplicates: Some(canonicalize),
    }
    .deserialize(&mut deserializer);
    match result {
        Ok(()) => Ok(()),
        Err(error) => {
            if matches!(
                error.classify(),
                serde_json::error::Category::Syntax | serde_json::error::Category::Eof
            ) {
                *syntax_error = true;
            }
            Err("value has an incompatible logical type")
        }
    }
}

/// Whether the type tree of a selected field can contain JSON number tokens.
///
/// The wide-integer scan below only matters where a number token can reach the
/// assembled row: numeric scalars and composites. A `Utf8` (or temporal,
/// boolean, binary, null) field's value is a single string-like token — a
/// digit run inside it is just text, the retained Polars JsonReader decodes it
/// as a string, and the generic path's re-encoding differs from the raw bytes
/// only in escape spellings that decode identically. Known non-number types
/// are listed explicitly; the conservative default (`true`) keeps unknown
/// future types scanned, so a newly added numeric type can never silently skip
/// the check.
#[allow(clippy::match_like_matches_macro)]
fn type_can_contain_number_tokens(data_type: &LogicalType) -> bool {
    !matches!(
        data_type,
        LogicalType::Utf8
            | LogicalType::Binary
            | LogicalType::Boolean
            | LogicalType::Date32
            | LogicalType::Timestamp { .. }
            | LogicalType::Null
    )
}

fn contains_timestamp(data_type: &LogicalType) -> bool {
    match data_type {
        LogicalType::Timestamp { .. } => true,
        LogicalType::List(element) => contains_timestamp(element),
        LogicalType::Struct(fields) => fields
            .iter()
            .any(|field| contains_timestamp(&field.data_type)),
        _ => false,
    }
}

/// Conservative detector for integer literals whose magnitude cannot survive
/// serde_json's own integer parse: any run of >= 19 consecutive ASCII digits.
///
/// The generic path parses such tokens through `serde_json::Value`, which
/// promotes them to `f64` (or stores an unsigned form) and re-encodes them in
/// that normalized form; the retained Polars JsonReader then rejects the raw
/// integer spelling on float columns (e.g. `1000000000000000000000000000000`
/// on a Float64 field). Canonicalizing these captures reproduces the generic
/// bytes exactly. Digit runs inside strings over-approximate — the only cost
/// is one bounded owned copy for that value, and the decoded result is
/// unchanged — so the check deliberately needs no string-awareness.
fn has_wide_integer_literal(text: &str) -> bool {
    let mut run = 0_usize;
    for &byte in text.as_bytes() {
        if byte.is_ascii_digit() {
            run += 1;
            if run >= 19 {
                return true;
            }
        } else {
            run = 0;
        }
    }
    false
}

/// Records the captured slice as a borrowed span into the framed row.
///
/// `serde_json`'s raw-value capture borrows from the deserializer input, which
/// is exactly the framed row `raw` this assembler validates, so the value's
/// address range is recorded as offsets from the row's start and re-checked
/// against the row at assembly time. No bytes are copied.
fn record_raw_span(
    captured: &RawValue,
    base: usize,
    target: &mut ProjectedSlot,
) -> Result<(), &'static str> {
    let text = captured.get();
    let start =
        usize::checked_sub(text.as_ptr() as usize, base).ok_or("captured value escaped its row")?;
    let end = start
        .checked_add(text.len())
        .ok_or("captured value escaped its row")?;
    *target = ProjectedSlot::Raw { start, end };
    Ok(())
}

/// Rebuilds the exact generic-path DOM form of a captured selected subtree and
/// re-serializes it compactly.
///
/// This is the one disclosed owned copy on the whole path: bounded by the
/// captured subtree's own bytes, dropped with the row, and only reached for
/// duplicate-key or raw-control-byte captures. The rebuilt `serde_json::Value`
/// is pushed through the generic oracle (`read::validate_json_value`) so the
/// canonicalized form can never bypass validation, and its compact
/// re-encoding is byte-identical to the generic path's re-serialization of the
/// same value.
fn canonical_captured_value(
    text: &str,
    field: &LogicalField,
) -> Result<ProjectedSlot, &'static str> {
    let dom: serde_json::Value =
        serde_json::from_str(text).map_err(|_| "value has an incompatible logical type")?;
    // The rebuilt form can never bypass validation: the generic oracle decides.
    validate_json_value(&dom, field)?;
    let mut dom = dom;
    crate::read::normalize_json_temporal_value(&mut dom, &field.data_type)?;
    let encoded = serde_json::to_vec(&dom).map_err(|_| "value has an incompatible logical type")?;
    Ok(ProjectedSlot::Canonical(encoded))
}
