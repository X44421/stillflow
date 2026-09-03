//! Q-R1 bounded deterministic streaming profiler (ADR-003 §§3–6 and §9;
//! issue #178). Realizes `profile_report.v1` as an exact, order-independent
//! function of the scan scope: no sampling, no approximation, no hidden
//! unbounded state. Canonical body/digest semantics live here; persistence,
//! history, and API ownership remain downstream (E5/Q-D1/Q-A1).

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{
    Array as _, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, RecordBatch, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow_schema::DataType;
use futures::{Stream, StreamExt};
use sha2::{Digest as _, Sha256};
use stillflow_connectors::InspectRequest;
use stillflow_core::{
    BatchEnvelope, LogicalSchema, LogicalType, ReadRequest, RequestContext, SourceAsset,
    SourceConnection, TimeUnit, MAX_BATCH_BYTES,
};
use stillflow_storage::SnapshotStore;
use uuid::Uuid;

use crate::error::{map_context_error, EngineError};
use crate::verification::{encode_component, KeyBytes, KeyValue};
use crate::{
    ExecutionEngine, PROFILE_DEFAULT_HISTOGRAM_BUCKETS, PROFILE_DEFAULT_TOP_K, PROFILE_MAX_COLUMNS,
    PROFILE_MAX_DISTINCT_ENTRIES_PER_COLUMN, PROFILE_MAX_FULL_ROW_DISTINCT_ENTRIES,
    PROFILE_MAX_HISTOGRAM_BUCKETS, PROFILE_MAX_RETAINED_VALUE_BYTES, PROFILE_MAX_ROWS,
    PROFILE_MAX_SCAN_BYTES, PROFILE_MAX_TOP_K, PROFILING_CONTRACT_VERSION,
};

// ---------------------------------------------------------------------------
// Frozen contract constants (ADR-003 §3.1)
// ---------------------------------------------------------------------------

/// Deterministic retained-state ceiling for the exact per-value and full-row
/// distinct structures (issue #178 bounded-memory law). Derivable bound:
/// per-column state ≤ `PROFILE_MAX_DISTINCT_ENTRIES_PER_COLUMN` entries,
/// full-row state ≤ `PROFILE_MAX_FULL_ROW_DISTINCT_ENTRIES` entries, each
/// additionally charged by encoded byte volume against this budget. Budget
/// exhaustion is the existing typed resource-limit failure
/// (`EngineError::BoundExceeded`), never an approximation.
pub const PROFILE_STATE_BYTE_BUDGET: usize = MAX_BATCH_BYTES;

/// Fixed Utf8/Binary length-histogram upper bounds (ADR-003 §6.3) plus the
/// final open bucket `[4096, ∞)`.
const LENGTH_HISTOGRAM_BOUNDS: [u64; 13] = [0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 4096];
const LENGTH_HISTOGRAM_BUCKETS: usize = LENGTH_HISTOGRAM_BOUNDS.len() + 1;

const PROFILE_BATCH_SIZE: usize = 4096;

// ---------------------------------------------------------------------------
// Request
// ---------------------------------------------------------------------------

/// Column selection: all columns in schema order, or an explicit ordered list
/// (caller order preserved; part of the determinism inputs).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProfileColumns {
    All,
    Explicit(Vec<String>),
}

/// One Q-R1 profile request (ADR-003 §3.2): target reference, column
/// selection, top_k, histogram_buckets. No sampling parameter exists in v1.
#[derive(Debug, Clone)]
pub struct ProfileRequest {
    pub connection: SourceConnection,
    pub asset: SourceAsset,
    pub columns: ProfileColumns,
    pub top_k: usize,
    pub histogram_buckets: usize,
    pub context: RequestContext,
}

impl ProfileRequest {
    /// Validates the frozen bounds and applies the contract defaults. A
    /// request without a deadline is a typed validation error before
    /// execution (ADR-003 §3.1 deadline law).
    pub fn new(
        connection: SourceConnection,
        asset: SourceAsset,
        columns: ProfileColumns,
        top_k: Option<usize>,
        histogram_buckets: Option<usize>,
        context: RequestContext,
    ) -> Result<Self, EngineError> {
        if context.deadline().is_none() {
            return Err(EngineError::InvalidPlan(
                "profile run requires a request deadline",
            ));
        }
        let top_k = top_k.unwrap_or(PROFILE_DEFAULT_TOP_K);
        let histogram_buckets = histogram_buckets.unwrap_or(PROFILE_DEFAULT_HISTOGRAM_BUCKETS);
        if top_k == 0 || top_k > PROFILE_MAX_TOP_K {
            return Err(EngineError::BoundExceeded(
                "profile top_k is outside the authorized range",
            ));
        }
        if histogram_buckets == 0 || histogram_buckets > PROFILE_MAX_HISTOGRAM_BUCKETS {
            return Err(EngineError::BoundExceeded(
                "profile histogram_buckets is outside the authorized range",
            ));
        }
        if let ProfileColumns::Explicit(names) = &columns {
            let mut sorted = names.clone();
            sorted.sort();
            if sorted.windows(2).any(|window| window[0] == window[1]) {
                return Err(EngineError::InvalidPlan(
                    "profile column selection contains duplicates",
                ));
            }
        }
        Ok(Self {
            connection,
            asset,
            columns,
            top_k,
            histogram_buckets,
            context,
        })
    }
}

// ---------------------------------------------------------------------------
// Result model
// ---------------------------------------------------------------------------

/// A float-domain value recorded after `-0.0 → +0.0` normalization; rendered
/// in the canonical body as `{"$float": "<16 uppercase hex digits>"}`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ProfileFloat(pub f64);

/// Exact reduced rational `(numerator, denominator)`, `denominator ≥ 1`,
/// `gcd = 1`; zero is `(0, 1)` (ADR-003 §4/§9).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProfileRational {
    pub numerator: i128,
    pub denominator: u128,
}

impl ProfileRational {
    fn new(numerator: i128, denominator: u128) -> Self {
        let sign = if numerator < 0 { -1i128 } else { 1 };
        let n = numerator.unsigned_abs();
        let g = gcd_u128(n, denominator);
        let (n, d) = if g == 0 {
            (n, denominator)
        } else {
            (n / g, denominator / g)
        };
        Self {
            numerator: sign * n as i128,
            denominator: d,
        }
    }
}

fn gcd_u128(a: u128, b: u128) -> u128 {
    if b == 0 {
        a
    } else {
        gcd_u128(b, a % b)
    }
}

/// One top-value pair (ADR-003 §6.2): count descending, ties by value
/// ascending in unsigned lexicographic byte order.
#[derive(Debug, Clone, PartialEq)]
pub enum ProfileTopValue {
    Text { value: String, count: u64 },
    Bytes { value: Vec<u8>, count: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProfileColumnStatus {
    Profiled,
    SkippedUnsupportedType,
}

/// Order-extreme value in canonical form (ADR-003 §9 temporal/float tags).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ProfileExtreme {
    Int(i128),
    Float(ProfileFloat),
    DateDays(i32),
    EpochMs(i64),
    EpochUs(i64),
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ProfileLengthStats {
    pub sum_of_lengths: u128,
    pub min_length: Option<u64>,
    pub max_length: Option<u64>,
    pub avg_length: Option<ProfileRational>,
    pub long_value_count: u64,
    pub histogram: Vec<u64>,
}

/// Numeric distribution (ADR-003 §6.1). `min`/`max`/`width` are recorded
/// bit-exact so bucket membership is recomputable from the artifact alone.
#[derive(Debug, Clone, PartialEq)]
pub struct ProfileHistogram {
    pub float_domain: bool,
    pub min: ProfileFloat,
    pub max: ProfileFloat,
    pub width: ProfileFloat,
    pub counts: Vec<u64>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct DatasetMetrics {
    pub row_count_scanned: u64,
    pub column_count_profiled: usize,
    pub scanned_bytes: u64,
    pub truncated: bool,
    pub distinct_row_count: Option<u64>,
    pub duplicate_row_count: Option<u64>,
    pub full_row_distinct_overflow: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnProfile {
    pub name: String,
    pub logical_type: String,
    pub status: ProfileColumnStatus,
    pub null_count: u64,
    pub non_null_count: u64,
    pub unique_count: Option<u64>,
    pub distinct_overflow: bool,
    pub empty_count: Option<u64>,
    pub min_value: Option<ProfileExtreme>,
    pub max_value: Option<ProfileExtreme>,
    pub sum: Option<ProfileSum>,
    pub mean: Option<ProfileMean>,
    pub sum_overflow: bool,
    pub non_finite_count: Option<u64>,
    pub true_count: Option<u64>,
    pub false_count: Option<u64>,
    pub length: Option<ProfileLengthStats>,
    pub histogram: Option<ProfileHistogram>,
    pub top_values: Option<Vec<ProfileTopValue>>,
}

/// Column sum: exact i128 for integer columns (absent with `sum_overflow` on
/// overflow) and a float-domain value for float columns.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ProfileSum {
    Int(i128),
    Float(ProfileFloat),
}

/// Column mean: exact rational for integer columns, float-domain value for
/// float columns (ADR-003 §4).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ProfileMean {
    Rational(ProfileRational),
    Float(ProfileFloat),
}

#[derive(Debug, Clone, PartialEq)]
pub struct DatasetProfile {
    pub artifact_type: &'static str,
    pub artifact_body_version: u16,
    pub profiling_contract_version: u16,
    pub dataset: DatasetMetrics,
    pub columns: Vec<ColumnProfile>,
}

/// The Q-R1 result: the typed profile plus its canonical body bytes and
/// lowercase-hex SHA-256 digest. The run identifier is envelope metadata
/// outside the canonical body (ADR-003 §9).
#[derive(Debug, Clone)]
pub struct ProfileResult {
    pub run_id: Uuid,
    pub profile: DatasetProfile,
    pub canonical_body: Vec<u8>,
    pub canonical_digest: String,
}

// ---------------------------------------------------------------------------
// Exact float accumulation (partition-invariance law, ADR-003 §6.1)
// ---------------------------------------------------------------------------

/// Exact signed fixed-point accumulator with the binary point at `2^-1074`
/// (the least f64 subnormal). Every f64 is an integer multiple of `2^-1074`,
/// so accumulating the value's mantissa into big magnitudes — positives and
/// negatives separately — is exact, and integer addition is commutative and
/// associative: identical multisets produce identical magnitudes regardless
/// of batch partitioning. Finalization rounds the exact sum (or the exact
/// quotient for the mean) to the nearest f64, ties to even. Naive sequential
/// f64 accumulation is never used.
#[derive(Default)]
struct ExactFloatSum {
    positive: BigMag,
    negative: BigMag,
}

#[derive(Default, Clone)]
struct BigMag {
    /// Little-endian u32 limbs; bit `i` is the coefficient of `2^(i - 1074)`.
    limbs: Vec<u32>,
}

impl BigMag {
    fn add_mantissa(&mut self, mantissa: u64, grid_exp: i32) {
        let bit = (grid_exp + 1074) as u32;
        let limb = (bit / 32) as usize;
        let shift = bit % 32;
        if self.limbs.len() < limb + 3 {
            self.limbs.resize(limb + 3, 0);
        }
        let mut acc = (mantissa as u128) << shift;
        let mut index = limb;
        while acc != 0 {
            if index >= self.limbs.len() {
                self.limbs.resize(index + 1, 0);
            }
            let sum = self.limbs[index] as u128 + (acc & 0xFFFF_FFFF);
            self.limbs[index] = sum as u32;
            acc >>= 32;
            acc += sum >> 32;
            index += 1;
        }
    }

    fn bit(&self, index: usize) -> bool {
        match self.limbs.get(index / 32) {
            Some(limb) => (limb >> (index % 32)) & 1 == 1,
            None => false,
        }
    }

    fn set_bit(&mut self, index: usize) {
        let limb = index / 32;
        if self.limbs.len() <= limb {
            self.limbs.resize(limb + 1, 0);
        }
        self.limbs[limb] |= 1 << (index % 32);
    }

    fn clear_bit(&mut self, index: usize) {
        let limb = index / 32;
        if let Some(value) = self.limbs.get_mut(limb) {
            *value &= !(1 << (index % 32));
        }
    }

    fn is_zero(&self) -> bool {
        self.limbs.iter().all(|limb| *limb == 0)
    }

    fn top_bit(&self) -> Option<usize> {
        for index in (0..self.limbs.len()).rev() {
            if self.limbs[index] != 0 {
                return Some(index * 32 + 31 - self.limbs[index].leading_zeros() as usize);
            }
        }
        None
    }

    fn cmp(&self, other: &BigMag) -> std::cmp::Ordering {
        match self.top_bit().cmp(&other.top_bit()) {
            std::cmp::Ordering::Equal => {}
            non_eq => return non_eq,
        }
        let Some(top) = self.top_bit() else {
            return std::cmp::Ordering::Equal;
        };
        for index in (0..=top).rev() {
            match self.bit(index).cmp(&other.bit(index)) {
                std::cmp::Ordering::Equal => {}
                non_eq => return non_eq,
            }
        }
        std::cmp::Ordering::Equal
    }

    fn subtract(&mut self, other: &BigMag) {
        // self -= other; requires self ≥ other (callers compare first).
        let top = self.top_bit().unwrap_or(0);
        let mut borrow = false;
        for index in 0..=top {
            let mut a = self.bit(index);
            let b = other.bit(index);
            if borrow {
                a = !a;
                // when a flips to 1 the borrow is absorbed; the match below
                // re-derives the outgoing borrow from (a, b).
            }
            let (diff, new_borrow) = match (a, b) {
                (false, true) => (true, true),
                (true, true) | (false, false) => (false, false),
                (true, false) => (true, false),
            };
            if diff {
                self.set_bit(index);
            } else {
                self.clear_bit(index);
            }
            borrow = new_borrow;
        }
    }
}

impl ExactFloatSum {
    fn add_f64(&mut self, value: f64) {
        if value == 0.0 {
            return; // ±0.0 contributes nothing (-0.0 normalized by caller).
        }
        let bits = value.to_bits();
        let negative = bits >> 63 == 1;
        let exp_bits = ((bits >> 52) & 0x7ff) as i32;
        let fraction = bits & ((1u64 << 52) - 1);
        let (mantissa, grid_exp) = if exp_bits == 0 {
            (fraction, -1074i32)
        } else {
            (fraction | (1u64 << 52), exp_bits - 1075)
        };
        if negative {
            self.negative.add_mantissa(mantissa, grid_exp);
        } else {
            self.positive.add_mantissa(mantissa, grid_exp);
        }
    }

    fn exact_magnitude(&self) -> (BigMag, bool) {
        match self.positive.cmp(&self.negative) {
            std::cmp::Ordering::Equal => (BigMag::default(), false),
            std::cmp::Ordering::Greater => {
                let mut magnitude = self.positive.clone();
                magnitude.subtract(&self.negative);
                (magnitude, false)
            }
            std::cmp::Ordering::Less => {
                let mut magnitude = self.negative.clone();
                magnitude.subtract(&self.positive);
                (magnitude, true)
            }
        }
    }

    /// Exact sum rounded to the nearest f64 (ties to even).
    fn finalize_sum(&self) -> f64 {
        let (magnitude, negative) = self.exact_magnitude();
        magnitude_to_f64(&magnitude, 1074, negative)
    }

    /// Exact mean = (exact sum) / `count`, rounded to the nearest f64 via
    /// long division with guard/sticky bits.
    fn finalize_mean(&self, count: u64) -> f64 {
        let (magnitude, negative) = self.exact_magnitude();
        if magnitude.is_zero() {
            return 0.0;
        }
        let top = magnitude.top_bit().unwrap_or(0);
        let mut kept: u64 = 0;
        let mut remainder: u64 = 0;
        let mut produced = 0usize;
        let mut sticky = false;
        for position in (0..=top).rev() {
            remainder = remainder
                .wrapping_mul(2)
                .wrapping_add(magnitude.bit(position) as u64);
            let quotient_bit = remainder >= count;
            if quotient_bit {
                remainder -= count;
            }
            if produced < 54 {
                if quotient_bit {
                    kept |= 1u64 << (53 - produced);
                }
                produced += 1;
            } else if quotient_bit {
                sticky = true;
            }
        }
        if remainder != 0 {
            sticky = true;
        }
        // Guard round: bit 54 is the guard, everything below is sticky.
        let round_bit = kept & 1 == 1;
        kept >>= 1;
        if round_bit && (sticky || kept & 1 == 1) {
            kept += 1;
        }
        // value = kept × 2^(top - 53 - 1074); kept ∈ [2^52, 2^53] after the
        // guard (2^53 handled by the exponent bump in the converter).
        kept_to_f64(kept, top as i64 - 53 - 1074, negative)
    }
}

/// Converts a kept mantissa (53 or 54 bits) scaled by `2^exponent` into f64
/// with round-to-nearest-even normalization and subnormal handling.
fn kept_to_f64(mut kept: u64, exponent: i64, negative: bool) -> f64 {
    let mut value_exponent = exponent + (64 - kept.leading_zeros()) as i64 - 1;
    if kept >> 53 != 0 {
        kept >>= 1;
        value_exponent += 1;
    }
    while kept != 0 && kept >> 52 == 0 {
        kept <<= 1;
        value_exponent -= 1;
    }
    if kept == 0 {
        return 0.0;
    }
    let sign = if negative { 1u64 << 63 } else { 0 };
    if value_exponent > 1023 {
        return if negative {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        };
    }
    if value_exponent < -1022 {
        // Subnormal: mantissa = value / 2^-1074 = kept × 2^(exponent + 1074),
        // rounded to even. shift_right = -(exponent + 1074) when negative.
        let shift_right = (-1074 - value_exponent).max(0) as u32;
        let mantissa = if shift_right >= 64 {
            0u64 // magnitude below half the least subnormal rounds to zero
        } else {
            let round_bit = shift_right > 0 && (kept >> (shift_right - 1)) & 1 == 1;
            let mut sticky = false;
            for index in 0..shift_right.saturating_sub(1) {
                if (kept >> index) & 1 == 1 {
                    sticky = true;
                    break;
                }
            }
            let mut mantissa = kept >> shift_right;
            if round_bit && (sticky || mantissa & 1 == 1) {
                mantissa += 1;
            }
            mantissa
        };
        return f64::from_bits(sign | mantissa);
    }
    let biased = (value_exponent + 1023) as u64;
    let mantissa = kept & ((1u64 << 52) - 1);
    f64::from_bits(sign | (biased << 52) | mantissa)
}

/// Converts an exact integer magnitude times `2^-grid_shift` into f64 with
/// round-to-nearest-even. Subnormal magnitudes (top ≤ 51 for the f64 grid)
/// are exact: an integer multiple of `2^-1074` below `2^52` fits the 52-bit
/// subnormal mantissa without rounding.
fn magnitude_to_f64(magnitude: &BigMag, grid_shift: u32, negative: bool) -> f64 {
    let Some(top) = magnitude.top_bit() else {
        return 0.0;
    };
    let value_exponent = top as i64 - grid_shift as i64;
    let sign = if negative { 1u64 << 63 } else { 0 };
    if value_exponent > 1023 {
        return if negative {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        };
    }
    if value_exponent < -1022 {
        let mut mantissa = 0u64;
        for index in 0..=top {
            if magnitude.bit(index) {
                mantissa |= 1u64 << index;
            }
        }
        return f64::from_bits(sign | mantissa);
    }
    let shift = top + 1 - 53;
    let mut kept: u64 = 0;
    for index in (shift..=top).rev() {
        kept = (kept << 1) | magnitude.bit(index) as u64;
    }
    let round_bit = shift > 0 && magnitude.bit(shift - 1);
    let mut sticky = false;
    for index in 0..shift.saturating_sub(1) {
        if magnitude.bit(index) {
            sticky = true;
            break;
        }
    }
    if round_bit && (sticky || kept & 1 == 1) {
        kept += 1;
    }
    if kept >> 53 != 0 {
        // Rounding carried out of the top bit.
        let biased = (value_exponent + 1023) as u64 + 1;
        if biased >= 0x7ff {
            return if negative {
                f64::NEG_INFINITY
            } else {
                f64::INFINITY
            };
        }
        return f64::from_bits(sign | (biased << 52));
    }
    let biased = (value_exponent + 1023) as u64;
    let mantissa = kept & ((1u64 << 52) - 1);
    f64::from_bits(sign | (biased << 52) | mantissa)
}

// ---------------------------------------------------------------------------
// Canonical JSON (ADR-003 §9)
// ---------------------------------------------------------------------------

enum CVal {
    Bool(bool),
    Int(i128),
    Rat(i128, u128),
    Float(f64),
    Str(String),
    Bytes(Vec<u8>),
    DateDays(i32),
    EpochMs(i64),
    EpochUs(i64),
    Arr(Vec<CVal>),
    Obj(Vec<(&'static str, CVal)>),
}

fn write_canonical(value: &CVal, out: &mut Vec<u8>) {
    match value {
        CVal::Bool(true) => out.extend_from_slice(b"true"),
        CVal::Bool(false) => out.extend_from_slice(b"false"),
        CVal::Int(number) => out.extend_from_slice(number.to_string().as_bytes()),
        CVal::Rat(numerator, denominator) => {
            out.extend_from_slice(b"{\"denominator\":");
            out.extend_from_slice(denominator.to_string().as_bytes());
            out.extend_from_slice(b",\"numerator\":");
            out.extend_from_slice(numerator.to_string().as_bytes());
            out.push(b'}');
        }
        CVal::Float(number) => {
            out.extend_from_slice(b"{\"$float\":\"");
            out.extend_from_slice(format!("{:016X}", number.to_bits()).as_bytes());
            out.extend_from_slice(b"\"}");
        }
        CVal::Str(text) => write_json_string(text, out),
        CVal::Bytes(bytes) => {
            out.extend_from_slice(b"{\"$bytes\":\"");
            for byte in bytes {
                out.extend_from_slice(format!("{byte:02x}").as_bytes());
            }
            out.extend_from_slice(b"\"}");
        }
        CVal::DateDays(days) => {
            out.extend_from_slice(b"{\"$date_days\":");
            out.extend_from_slice(days.to_string().as_bytes());
            out.push(b'}');
        }
        CVal::EpochMs(epoch) => {
            out.extend_from_slice(b"{\"$epoch_ms\":");
            out.extend_from_slice(epoch.to_string().as_bytes());
            out.push(b'}');
        }
        CVal::EpochUs(epoch) => {
            out.extend_from_slice(b"{\"$epoch_us\":");
            out.extend_from_slice(epoch.to_string().as_bytes());
            out.push(b'}');
        }
        CVal::Arr(items) => {
            out.push(b'[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                write_canonical(item, out);
            }
            out.push(b']');
        }
        CVal::Obj(entries) => {
            let mut sorted: Vec<&(&'static str, CVal)> = entries.iter().collect();
            sorted.sort_by(|left, right| left.0.cmp(right.0));
            out.push(b'{');
            for (index, (key, item)) in sorted.iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                write_json_string(key, out);
                out.push(b':');
                write_canonical(item, out);
            }
            out.push(b'}');
        }
    }
}

/// Pinned JSON string escaping (ADR-003 §9): only `"`, `\`, and
/// U+0000–U+001F are escaped (short escapes where they apply, else
/// `\u00xx` lowercase); everything else is emitted directly as UTF-8.
fn write_json_string(text: &str, out: &mut Vec<u8>) {
    out.push(b'"');
    for character in text.chars() {
        match character {
            '"' => out.extend_from_slice(b"\\\""),
            '\\' => out.extend_from_slice(b"\\\\"),
            '\u{8}' => out.extend_from_slice(b"\\b"),
            '\u{9}' => out.extend_from_slice(b"\\t"),
            '\u{a}' => out.extend_from_slice(b"\\n"),
            '\u{c}' => out.extend_from_slice(b"\\f"),
            '\u{d}' => out.extend_from_slice(b"\\r"),
            control if (control as u32) < 0x20 => {
                out.extend_from_slice(format!("\\u{:04x}", control as u32).as_bytes());
            }
            other => out.extend_from_slice(other.to_string().as_bytes()),
        }
    }
    out.push(b'"');
}

impl DatasetProfile {
    /// Canonical `profile_report.v1` body bytes (ADR-003 §9): UTF-8 JSON with
    /// lexicographically sorted keys, no insignificant whitespace, pinned
    /// scalar encodings, and no provenance/wall-clock fields.
    pub fn canonical_body(&self) -> Vec<u8> {
        let mut columns = Vec::new();
        for column in &self.columns {
            let mut entries: Vec<(&'static str, CVal)> = vec![
                ("name", CVal::Str(column.name.clone())),
                ("non_null_count", CVal::Int(column.non_null_count as i128)),
                ("null_count", CVal::Int(column.null_count as i128)),
                (
                    "status",
                    CVal::Str(match column.status {
                        ProfileColumnStatus::Profiled => "profiled".to_owned(),
                        ProfileColumnStatus::SkippedUnsupportedType => {
                            "skipped_unsupported_type".to_owned()
                        }
                    }),
                ),
                ("type", CVal::Str(column.logical_type.clone())),
            ];
            if column.distinct_overflow {
                entries.push(("distinct_overflow", CVal::Bool(true)));
            }
            if let Some(unique) = column.unique_count {
                entries.push(("unique_count", CVal::Int(unique as i128)));
            }
            if let Some(empty) = column.empty_count {
                entries.push(("empty_count", CVal::Int(empty as i128)));
            }
            if let Some(minimum) = &column.min_value {
                entries.push(("min_value", extreme_value(minimum)));
            }
            if let Some(maximum) = &column.max_value {
                entries.push(("max_value", extreme_value(maximum)));
            }
            match column.sum {
                Some(ProfileSum::Int(sum)) => entries.push(("sum", CVal::Int(sum))),
                Some(ProfileSum::Float(sum)) => entries.push(("sum", CVal::Float(sum.0))),
                None => {}
            }
            if column.sum_overflow {
                entries.push(("sum_overflow", CVal::Bool(true)));
            }
            match column.mean {
                Some(ProfileMean::Rational(rational)) => {
                    entries.push(("mean", CVal::Rat(rational.numerator, rational.denominator)));
                }
                Some(ProfileMean::Float(float)) => entries.push(("mean", CVal::Float(float.0))),
                None => {}
            }
            if let Some(non_finite) = column.non_finite_count {
                entries.push(("non_finite_count", CVal::Int(non_finite as i128)));
            }
            if let Some(true_count) = column.true_count {
                entries.push(("true_count", CVal::Int(true_count as i128)));
            }
            if let Some(false_count) = column.false_count {
                entries.push(("false_count", CVal::Int(false_count as i128)));
            }
            if let Some(length) = &column.length {
                entries.push((
                    "length_stats",
                    CVal::Obj(vec![
                        (
                            "avg_length",
                            match length.avg_length {
                                Some(average) => CVal::Rat(average.numerator, average.denominator),
                                None => CVal::Bool(false),
                            },
                        ),
                        (
                            "long_value_count",
                            CVal::Int(length.long_value_count as i128),
                        ),
                        (
                            "max_length",
                            CVal::Int(length.max_length.unwrap_or(0) as i128),
                        ),
                        (
                            "min_length",
                            CVal::Int(length.min_length.unwrap_or(0) as i128),
                        ),
                        (
                            "sum_of_lengths",
                            CVal::Int(i128::try_from(length.sum_of_lengths).unwrap_or(i128::MAX)),
                        ),
                    ]),
                ));
                entries.push((
                    "length_histogram",
                    CVal::Arr(
                        length
                            .histogram
                            .iter()
                            .map(|c| CVal::Int(*c as i128))
                            .collect(),
                    ),
                ));
            }
            if let Some(histogram) = &column.histogram {
                // Float histograms record the frozen edge inputs (min/max/
                // width) bit-exact per section 9; integer histograms are the
                // exact counts only (their edges are min_value/max_value).
                if histogram.float_domain {
                    entries.push((
                        "histogram",
                        CVal::Obj(vec![
                            (
                                "counts",
                                CVal::Arr(
                                    histogram
                                        .counts
                                        .iter()
                                        .map(|c| CVal::Int(*c as i128))
                                        .collect(),
                                ),
                            ),
                            ("max", CVal::Float(histogram.max.0)),
                            ("min", CVal::Float(histogram.min.0)),
                            ("width", CVal::Float(histogram.width.0)),
                        ]),
                    ));
                } else {
                    entries.push((
                        "histogram",
                        CVal::Arr(
                            histogram
                                .counts
                                .iter()
                                .map(|c| CVal::Int(*c as i128))
                                .collect(),
                        ),
                    ));
                }
            }
            if let Some(top_values) = &column.top_values {
                entries.push((
                    "top_values",
                    CVal::Arr(
                        top_values
                            .iter()
                            .map(|top| match top {
                                ProfileTopValue::Text { value, count } => CVal::Obj(vec![
                                    ("count", CVal::Int(*count as i128)),
                                    ("value", CVal::Str(value.clone())),
                                ]),
                                ProfileTopValue::Bytes { value, count } => CVal::Obj(vec![
                                    ("count", CVal::Int(*count as i128)),
                                    ("value", CVal::Bytes(value.clone())),
                                ]),
                            })
                            .collect(),
                    ),
                ));
            }
            columns.push(CVal::Obj(entries));
        }
        let mut dataset: Vec<(&'static str, CVal)> = vec![
            (
                "column_count_profiled",
                CVal::Int(self.dataset.column_count_profiled as i128),
            ),
            (
                "full_row_distinct_overflow",
                CVal::Bool(self.dataset.full_row_distinct_overflow),
            ),
            (
                "row_count_scanned",
                CVal::Int(self.dataset.row_count_scanned as i128),
            ),
            // scanned_bytes is envelope-packaging disclosure (ADR-003 §5):
            // it legitimately differs across partitionings of the same rows,
            // so it stays in the typed result and out of the canonical body
            // — the same rule that excludes run ids and wall clock.
            ("truncated", CVal::Bool(self.dataset.truncated)),
        ];
        if let Some(distinct) = self.dataset.distinct_row_count {
            dataset.push(("distinct_row_count", CVal::Int(distinct as i128)));
        }
        if let Some(duplicates) = self.dataset.duplicate_row_count {
            dataset.push(("duplicate_row_count", CVal::Int(duplicates as i128)));
        }
        let body = CVal::Obj(vec![
            (
                "artifact_body_version",
                CVal::Int(self.artifact_body_version as i128),
            ),
            ("artifact_type", CVal::Str(self.artifact_type.to_owned())),
            ("columns", CVal::Arr(columns)),
            (
                "profiling_contract_version",
                CVal::Int(self.profiling_contract_version as i128),
            ),
            ("dataset", CVal::Obj(dataset)),
        ]);
        let mut out = Vec::new();
        write_canonical(&body, &mut out);
        out
    }

    /// Lowercase-hex SHA-256 of the canonical body (ADR-003 §9).
    pub fn canonical_digest(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.canonical_body());
        hex_lower(&hasher.finalize())
    }
}

fn extreme_value(value: &ProfileExtreme) -> CVal {
    match value {
        ProfileExtreme::Int(number) => CVal::Int(*number),
        ProfileExtreme::Float(number) => CVal::Float(number.0),
        ProfileExtreme::DateDays(days) => CVal::DateDays(*days),
        ProfileExtreme::EpochMs(epoch) => CVal::EpochMs(*epoch),
        ProfileExtreme::EpochUs(epoch) => CVal::EpochUs(*epoch),
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

// ---------------------------------------------------------------------------
// Streaming accumulators
// ---------------------------------------------------------------------------

#[derive(Default)]
struct TextState {
    empty_count: u64,
    length_sum: u128,
    length_min: Option<u64>,
    length_max: Option<u64>,
    long_value_count: u64,
    length_histogram: Vec<u64>,
    /// Exact per-value counts keyed by the raw value bytes (bijective within
    /// one column; also drives top-K ordering).
    distinct: BTreeMap<Vec<u8>, u64>,
}

#[derive(Default)]
struct IntegerState {
    min: Option<i128>,
    max: Option<i128>,
    sum: i128,
    sum_overflow: bool,
    /// Exact per-value counts keyed by the big-endian i128 value bytes.
    distinct: BTreeMap<[u8; 16], u64>,
}

#[derive(Default)]
struct FloatState {
    min: Option<f64>,
    max: Option<f64>,
    non_finite_count: u64,
    exact_sum: ExactFloatSum,
    finite_count: u64,
    /// Exact per-value counts keyed by the normalized bit pattern (NaN
    /// payloads group; -0.0 normalizes to +0.0), mirroring the E4 encoder's
    /// float semantics.
    distinct: BTreeMap<u64, u64>,
}

#[derive(Default)]
struct TemporalState {
    min: Option<i64>,
    max: Option<i64>,
    distinct: BTreeMap<[u8; 8], u64>,
}

enum ColumnState {
    Skipped,
    Boolean {
        true_count: u64,
        false_count: u64,
        seen_true: bool,
        seen_false: bool,
    },
    Integer(IntegerState),
    Float(FloatState),
    Date(TemporalState),
    Timestamp(TemporalState),
    Text(TextState),
    Binary(TextState),
}

struct ColumnRuntime {
    name: String,
    logical_type: &'static str,
    state: ColumnState,
    null_count: u64,
    distinct_overflow: bool,
    /// Retained distinct-state byte volume charged to the budget.
    state_bytes: usize,
}

fn length_bucket(length: u64) -> usize {
    for (index, bound) in LENGTH_HISTOGRAM_BOUNDS.iter().enumerate() {
        if length <= *bound {
            return index;
        }
    }
    LENGTH_HISTOGRAM_BUCKETS - 1
}

fn normalize_float(value: f64) -> f64 {
    if value == 0.0 {
        0.0
    } else {
        value
    }
}

fn float_distinct_key(value: f64) -> u64 {
    // Mirrors the E4 canonical float semantics: NaN payloads group together
    // and -0.0 normalizes to +0.0 (verification.rs key-encoding tests).
    if value.is_nan() {
        0x7ff8_0000_0000_0000
    } else {
        normalize_float(value).to_bits()
    }
}

fn charge(state_bytes: &mut usize, bytes: usize) -> Result<(), EngineError> {
    *state_bytes += bytes;
    if *state_bytes > PROFILE_STATE_BYTE_BUDGET {
        return Err(EngineError::BoundExceeded(
            "profile retained distinct state exceeded the deterministic memory budget",
        ));
    }
    Ok(())
}

impl ColumnRuntime {
    fn observe(&mut self, batch: &RecordBatch, column_index: usize) -> Result<(), EngineError> {
        let array = batch.column(column_index);
        self.null_count += array.null_count() as u64;
        match &mut self.state {
            ColumnState::Skipped => Ok(()),
            ColumnState::Boolean {
                true_count,
                false_count,
                seen_true,
                seen_false,
            } => {
                if let Some(values) = array.as_any().downcast_ref::<BooleanArray>() {
                    for index in 0..values.len() {
                        if values.is_null(index) {
                            continue;
                        }
                        if values.value(index) {
                            *true_count += 1;
                            *seen_true = true;
                        } else {
                            *false_count += 1;
                            *seen_false = true;
                        }
                    }
                }
                Ok(())
            }
            ColumnState::Integer(state) => {
                macro_rules! accumulate {
                    ($array_type:ty, $cast:expr) => {
                        if let Some(values) = array.as_any().downcast_ref::<$array_type>() {
                            for index in 0..values.len() {
                                if values.is_null(index) {
                                    continue;
                                }
                                let raw: i128 = $cast(values.value(index));
                                state.min = Some(state.min.map_or(raw, |m| m.min(raw)));
                                state.max = Some(state.max.map_or(raw, |m| m.max(raw)));
                                if !state.sum_overflow {
                                    let (sum, overflow) = state.sum.overflowing_add(raw);
                                    state.sum = sum;
                                    state.sum_overflow = overflow;
                                }
                                let key = raw.to_be_bytes();
                                let entry = state.distinct.entry(key).or_insert(0);
                                if *entry == 0 {
                                    charge(&mut self.state_bytes, 24)?;
                                }
                                *entry += 1;
                            }
                        }
                    };
                }
                match array.data_type() {
                    DataType::Int8 => accumulate!(Int8Array, |v: i8| v as i128),
                    DataType::Int16 => accumulate!(Int16Array, |v: i16| v as i128),
                    DataType::Int32 => accumulate!(Int32Array, |v: i32| v as i128),
                    DataType::Int64 => accumulate!(Int64Array, |v: i64| v as i128),
                    DataType::UInt8 => accumulate!(UInt8Array, |v: u8| v as i128),
                    DataType::UInt16 => accumulate!(UInt16Array, |v: u16| v as i128),
                    DataType::UInt32 => accumulate!(UInt32Array, |v: u32| v as i128),
                    DataType::UInt64 => accumulate!(UInt64Array, |v: u64| v as i128),
                    _ => {}
                }
                if state.distinct.len() > PROFILE_MAX_DISTINCT_ENTRIES_PER_COLUMN {
                    state.distinct.clear();
                    self.distinct_overflow = true;
                    self.state_bytes = 0;
                }
                Ok(())
            }
            ColumnState::Float(state) => {
                macro_rules! accumulate {
                    ($array_type:ty, $upcast:expr) => {
                        if let Some(values) = array.as_any().downcast_ref::<$array_type>() {
                            for index in 0..values.len() {
                                if values.is_null(index) {
                                    continue;
                                }
                                let raw: f64 = normalize_float($upcast(values.value(index)));
                                if raw.is_finite() {
                                    state.min = Some(state.min.map_or(raw, |m| m.min(raw)));
                                    state.max = Some(state.max.map_or(raw, |m| m.max(raw)));
                                    state.exact_sum.add_f64(raw);
                                    state.finite_count += 1;
                                } else {
                                    state.non_finite_count += 1;
                                }
                                let key = float_distinct_key(raw);
                                let entry = state.distinct.entry(key).or_insert(0);
                                if *entry == 0 {
                                    charge(&mut self.state_bytes, 16)?;
                                }
                                *entry += 1;
                            }
                        }
                    };
                }
                match array.data_type() {
                    DataType::Float32 => accumulate!(Float32Array, |v: f32| v as f64),
                    DataType::Float64 => accumulate!(Float64Array, |v: f64| v),
                    _ => {}
                }
                if state.distinct.len() > PROFILE_MAX_DISTINCT_ENTRIES_PER_COLUMN {
                    state.distinct.clear();
                    self.distinct_overflow = true;
                    self.state_bytes = 0;
                }
                Ok(())
            }
            ColumnState::Date(state) => {
                if let Some(values) = array.as_any().downcast_ref::<Date32Array>() {
                    for index in 0..values.len() {
                        if values.is_null(index) {
                            continue;
                        }
                        let raw = values.value(index) as i64;
                        state.min = Some(state.min.map_or(raw, |m| m.min(raw)));
                        state.max = Some(state.max.map_or(raw, |m| m.max(raw)));
                        let key = raw.to_be_bytes();
                        let entry = state.distinct.entry(key).or_insert(0);
                        if *entry == 0 {
                            charge(&mut self.state_bytes, 12)?;
                        }
                        *entry += 1;
                    }
                }
                Ok(())
            }
            ColumnState::Timestamp(state) => {
                macro_rules! accumulate {
                    ($array_type:ty) => {
                        if let Some(values) = array.as_any().downcast_ref::<$array_type>() {
                            for index in 0..values.len() {
                                if values.is_null(index) {
                                    continue;
                                }
                                let raw = values.value(index);
                                state.min = Some(state.min.map_or(raw, |m| m.min(raw)));
                                state.max = Some(state.max.map_or(raw, |m| m.max(raw)));
                                let key = raw.to_be_bytes();
                                let entry = state.distinct.entry(key).or_insert(0);
                                if *entry == 0 {
                                    charge(&mut self.state_bytes, 16)?;
                                }
                                *entry += 1;
                            }
                        }
                    };
                }
                match array.data_type() {
                    DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, _) => {
                        accumulate!(TimestampMillisecondArray)
                    }
                    DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, _) => {
                        accumulate!(TimestampMicrosecondArray)
                    }
                    DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, _) => {
                        accumulate!(TimestampNanosecondArray)
                    }
                    _ => {}
                }
                Ok(())
            }
            ColumnState::Text(state) => {
                if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
                    for index in 0..values.len() {
                        if values.is_null(index) {
                            continue;
                        }
                        let raw = values.value(index);
                        let bytes = raw.as_bytes();
                        let length = raw.chars().count() as u64;
                        if raw.is_empty() {
                            state.empty_count += 1;
                        }
                        state.length_sum += length as u128;
                        state.length_min = Some(state.length_min.map_or(length, |m| m.min(length)));
                        state.length_max = Some(state.length_max.map_or(length, |m| m.max(length)));
                        state.length_histogram[length_bucket(length)] += 1;
                        if bytes.len() > PROFILE_MAX_RETAINED_VALUE_BYTES {
                            state.long_value_count += 1;
                        }
                        let entry = state.distinct.entry(bytes.to_vec()).or_insert(0);
                        if *entry == 0 {
                            charge(&mut self.state_bytes, bytes.len() + 24)?;
                        }
                        *entry += 1;
                    }
                }
                if state.distinct.len() > PROFILE_MAX_DISTINCT_ENTRIES_PER_COLUMN {
                    state.distinct.clear();
                    self.distinct_overflow = true;
                    self.state_bytes = 0;
                }
                Ok(())
            }
            ColumnState::Binary(state) => {
                if let Some(values) = array.as_any().downcast_ref::<BinaryArray>() {
                    for index in 0..values.len() {
                        if values.is_null(index) {
                            continue;
                        }
                        let raw = values.value(index);
                        let length = raw.len() as u64;
                        if raw.is_empty() {
                            state.empty_count += 1;
                        }
                        state.length_sum += length as u128;
                        state.length_min = Some(state.length_min.map_or(length, |m| m.min(length)));
                        state.length_max = Some(state.length_max.map_or(length, |m| m.max(length)));
                        state.length_histogram[length_bucket(length)] += 1;
                        if raw.len() > PROFILE_MAX_RETAINED_VALUE_BYTES {
                            state.long_value_count += 1;
                        }
                        let entry = state.distinct.entry(raw.to_vec()).or_insert(0);
                        if *entry == 0 {
                            charge(&mut self.state_bytes, raw.len() + 24)?;
                        }
                        *entry += 1;
                    }
                }
                if state.distinct.len() > PROFILE_MAX_DISTINCT_ENTRIES_PER_COLUMN {
                    state.distinct.clear();
                    self.distinct_overflow = true;
                    self.state_bytes = 0;
                }
                Ok(())
            }
        }
    }
}

fn logical_type_name(logical: &LogicalType) -> &'static str {
    match logical {
        LogicalType::Null => "null",
        LogicalType::Boolean => "boolean",
        LogicalType::Int8 => "int8",
        LogicalType::Int16 => "int16",
        LogicalType::Int32 => "int32",
        LogicalType::Int64 => "int64",
        LogicalType::UInt8 => "uint8",
        LogicalType::UInt16 => "uint16",
        LogicalType::UInt32 => "uint32",
        LogicalType::UInt64 => "uint64",
        LogicalType::Float32 => "float32",
        LogicalType::Float64 => "float64",
        LogicalType::Utf8 => "utf8",
        LogicalType::Binary => "binary",
        LogicalType::Date32 => "date32",
        LogicalType::Timestamp {
            unit: TimeUnit::Millisecond,
            ..
        } => "timestamp_ms",
        LogicalType::Timestamp {
            unit: TimeUnit::Microsecond,
            ..
        } => "timestamp_us",
        LogicalType::Timestamp {
            unit: TimeUnit::Nanosecond,
            ..
        } => "timestamp_ns",
        LogicalType::Timestamp {
            unit: TimeUnit::Second,
            ..
        } => "timestamp_s",
        LogicalType::List(_) => "list",
        LogicalType::Struct(_) => "struct",
    }
}

fn column_state_for(logical: &LogicalType) -> ColumnState {
    match logical {
        LogicalType::Boolean => ColumnState::Boolean {
            true_count: 0,
            false_count: 0,
            seen_true: false,
            seen_false: false,
        },
        LogicalType::Int8
        | LogicalType::Int16
        | LogicalType::Int32
        | LogicalType::Int64
        | LogicalType::UInt8
        | LogicalType::UInt16
        | LogicalType::UInt32
        | LogicalType::UInt64 => ColumnState::Integer(IntegerState::default()),
        LogicalType::Float32 | LogicalType::Float64 => ColumnState::Float(FloatState::default()),
        LogicalType::Date32 => ColumnState::Date(TemporalState::default()),
        LogicalType::Timestamp { unit, .. } => match unit {
            TimeUnit::Millisecond | TimeUnit::Microsecond | TimeUnit::Nanosecond => {
                ColumnState::Timestamp(TemporalState::default())
            }
            // Timestamp-Second has no v1 canonical encoding (§9 defines
            // $epoch_ms/$epoch_us only); the column stays present with the
            // explicit skip status instead of inventing contract surface.
            TimeUnit::Second => ColumnState::Skipped,
        },
        LogicalType::Utf8 => ColumnState::Text(TextState {
            length_histogram: vec![0; LENGTH_HISTOGRAM_BUCKETS],
            ..TextState::default()
        }),
        LogicalType::Binary => ColumnState::Binary(TextState {
            length_histogram: vec![0; LENGTH_HISTOGRAM_BUCKETS],
            ..TextState::default()
        }),
        // Null/List/Struct are not v1 metric families; presence only.
        LogicalType::Null | LogicalType::List(_) | LogicalType::Struct(_) => ColumnState::Skipped,
    }
}

// ---------------------------------------------------------------------------
// Full-row distinct (exact framed keys, ADR-003 §5)
// ---------------------------------------------------------------------------

struct FullRowState {
    keys: BTreeMap<Vec<u8>, ()>,
    bytes: usize,
    overflow: bool,
}

impl FullRowState {
    fn observe(
        &mut self,
        batch: &RecordBatch,
        columns: &[(usize, LogicalType)],
    ) -> Result<(), EngineError> {
        if self.overflow {
            return Ok(());
        }
        for row in 0..batch.num_rows() {
            let mut key = KeyBytes::new();
            for (column_index, logical) in columns {
                let array = batch.column(*column_index);
                let is_null = array.is_null(row);
                // A Null-typed column and every null value share the single
                // zero-byte sentinel, distinct from every non-null encoding
                // (E4 key-encoding tests).
                if matches!(logical, LogicalType::Null) {
                    key.push(0x00)?;
                    continue;
                }
                // Timestamp-Second, List, and Struct have no v1 canonical
                // value encoding (§9 defines $epoch_ms/$epoch_us only).
                // Timestamp-Second keys are framed raw (reserved 0xFF tag +
                // big-endian epoch), lossless and deterministic under the
                // per-value length framing; List/Struct cannot be extracted
                // losslessly here and fail closed to the overflow flag.
                if matches!(
                    logical,
                    LogicalType::Timestamp {
                        unit: TimeUnit::Second,
                        ..
                    }
                ) {
                    if is_null {
                        key.push(0x00)?;
                    } else {
                        key.push(0xFF)?;
                        let epoch = array
                            .as_any()
                            .downcast_ref::<TimestampSecondArray>()
                            .ok_or(EngineError::Internal(
                                "profile key timestamp-second read failed",
                            ))?
                            .value(row);
                        key.extend_from_slice(&epoch.to_be_bytes())?;
                    }
                    continue;
                }
                if matches!(logical, LogicalType::List(_) | LogicalType::Struct(_)) {
                    self.overflow = true;
                    self.keys.clear();
                    self.bytes = 0;
                    return Ok(());
                }
                let value = if is_null {
                    KeyValue::Null
                } else {
                    arrow_key_value(array, row, logical)?
                };
                encode_component(logical, value, &mut key)?;
            }
            let encoded = key.as_slice().to_vec();
            if self.keys.insert(encoded.clone(), ()).is_none() {
                self.bytes += encoded.len() + 24;
                if self.keys.len() > PROFILE_MAX_FULL_ROW_DISTINCT_ENTRIES
                    || self.bytes > PROFILE_STATE_BYTE_BUDGET
                {
                    self.overflow = true;
                    self.keys.clear();
                    self.bytes = 0;
                    return Ok(());
                }
            }
        }
        Ok(())
    }
}

/// Builds the E4 canonical key value directly from an Arrow array, reusing
/// the tested `encode_component` (null sentinel, NaN grouping, -0.0
/// normalization) without any Polars conversion. The returned value borrows
/// `array`; the caller must encode it before that borrow ends.
fn arrow_key_value<'a>(
    array: &'a std::sync::Arc<dyn arrow_array::Array>,
    row: usize,
    logical: &LogicalType,
) -> Result<KeyValue<'a>, EngineError> {
    let mismatch = || EngineError::Internal("profile key component type mismatch");
    if array.is_null(row) {
        return Ok(KeyValue::Null);
    }
    macro_rules! primitive {
        ($array_type:ty, $variant:ident, $cast:expr) => {{
            let value = array
                .as_any()
                .downcast_ref::<$array_type>()
                .ok_or_else(mismatch)?
                .value(row);
            KeyValue::$variant($cast(value))
        }};
    }
    Ok(match (array.data_type(), logical) {
        (DataType::Boolean, LogicalType::Boolean) => {
            primitive!(BooleanArray, Boolean, |v: bool| v)
        }
        (DataType::Int8, LogicalType::Int8) => primitive!(Int8Array, Int8, |v: i8| v),
        (DataType::Int16, LogicalType::Int16) => primitive!(Int16Array, Int16, |v: i16| v),
        (DataType::Int32, LogicalType::Int32) => primitive!(Int32Array, Int32, |v: i32| v),
        (DataType::Int64, LogicalType::Int64) => primitive!(Int64Array, Int64, |v: i64| v),
        (DataType::UInt8, LogicalType::UInt8) => primitive!(UInt8Array, UInt8, |v: u8| v),
        (DataType::UInt16, LogicalType::UInt16) => primitive!(UInt16Array, UInt16, |v: u16| v),
        (DataType::UInt32, LogicalType::UInt32) => primitive!(UInt32Array, UInt32, |v: u32| v),
        (DataType::UInt64, LogicalType::UInt64) => primitive!(UInt64Array, UInt64, |v: u64| v),
        (DataType::Float32, LogicalType::Float32) => primitive!(Float32Array, Float32, |v: f32| v),
        (DataType::Float64, LogicalType::Float64) => primitive!(Float64Array, Float64, |v: f64| v),
        (DataType::Utf8, LogicalType::Utf8) => KeyValue::Utf8Owned(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(mismatch)?
                .value(row)
                .to_owned(),
        ),
        (DataType::Binary, LogicalType::Binary) => KeyValue::Binary(
            array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(mismatch)?
                .value(row),
        ),
        (DataType::Date32, LogicalType::Date32) => primitive!(Date32Array, Date32, |v: i32| v),
        (DataType::Timestamp(arrow_unit, _), LogicalType::Timestamp { unit, .. }) => {
            let epoch = match arrow_unit {
                arrow_schema::TimeUnit::Millisecond => array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(mismatch)?
                    .value(row),
                arrow_schema::TimeUnit::Microsecond => array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(mismatch)?
                    .value(row),
                arrow_schema::TimeUnit::Nanosecond => array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(mismatch)?
                    .value(row),
                arrow_schema::TimeUnit::Second => {
                    return Err(EngineError::Internal(
                        "timestamp-second keys have no v1 canonical encoding",
                    ));
                }
            };
            KeyValue::Timestamp {
                epoch,
                unit: *unit,
                timezone: None,
            }
        }
        _ => return Err(mismatch()),
    })
}

// ---------------------------------------------------------------------------
// Finalization
// ---------------------------------------------------------------------------

fn integer_histogram(
    buckets: usize,
    min: i128,
    max: i128,
    counts: &BTreeMap<[u8; 16], u64>,
) -> ProfileHistogram {
    let span = max - min;
    let mut out = vec![0u64; buckets];
    for (key, count) in counts {
        let value = i128::from_be_bytes(*key);
        let index = if span == 0 {
            0usize
        } else {
            // Exact i128 formula (ADR-003 §6.1); span > 0 and
            // 0 ≤ value − min ≤ span, so the truncating division is total
            // and cannot overflow for u64/i64-derived spans.
            let scaled = (value - min).saturating_mul(buckets as i128);
            (scaled.div_euclid(span) as usize).min(buckets - 1)
        };
        out[index] += *count;
    }
    ProfileHistogram {
        float_domain: false,
        min: ProfileFloat(min as f64),
        max: ProfileFloat(max as f64),
        width: ProfileFloat(0.0),
        counts: out,
    }
}

fn float_histogram(
    buckets: usize,
    min: f64,
    max: f64,
    counts: &BTreeMap<u64, u64>,
) -> ProfileHistogram {
    let width = (max - min) / buckets as f64;
    let mut out = vec![0u64; buckets];
    for (key, count) in counts {
        let value = f64::from_bits(*key);
        if !value.is_finite() {
            continue; // non-finite values are excluded from histograms
        }
        let index = if width == 0.0 {
            0usize
        } else if width.is_infinite() {
            // Infinite-width branch (ADR-003 §6.1): evaluated BEFORE the
            // general formula so a NaN intermediate cannot arise.
            if value == max {
                buckets - 1
            } else {
                0
            }
        } else {
            let position = ((value - min) / width).floor();
            if position < 0.0 {
                0
            } else {
                (position as usize).min(buckets - 1)
            }
        };
        out[index] += *count;
    }
    ProfileHistogram {
        float_domain: true,
        min: ProfileFloat(normalize_float(min)),
        max: ProfileFloat(normalize_float(max)),
        width: ProfileFloat(width),
        counts: out,
    }
}

impl ProfileRun {
    fn finish(mut self, top_k: usize, histogram_buckets: usize) -> DatasetProfile {
        let distinct_row_count = if self.full_row.overflow {
            None
        } else {
            Some(self.full_row.keys.len() as u64)
        };
        self.dataset.duplicate_row_count =
            distinct_row_count.map(|distinct| self.dataset.row_count_scanned - distinct);
        self.dataset.distinct_row_count = distinct_row_count;
        self.dataset.full_row_distinct_overflow = self.full_row.overflow;

        let mut columns = Vec::new();
        for runtime in self.columns.drain(..) {
            let state = runtime.state;
            let status = if matches!(state, ColumnState::Skipped) {
                ProfileColumnStatus::SkippedUnsupportedType
            } else {
                ProfileColumnStatus::Profiled
            };
            let non_null_count = self
                .dataset
                .row_count_scanned
                .saturating_sub(runtime.null_count);
            let mut column = ColumnProfile {
                name: runtime.name.clone(),
                logical_type: runtime.logical_type.to_owned(),
                status,
                null_count: runtime.null_count,
                non_null_count,
                unique_count: None,
                distinct_overflow: runtime.distinct_overflow,
                empty_count: None,
                min_value: None,
                max_value: None,
                sum: None,
                mean: None,
                sum_overflow: false,
                non_finite_count: None,
                true_count: None,
                false_count: None,
                length: None,
                histogram: None,
                top_values: None,
            };
            match state {
                ColumnState::Skipped => {}
                ColumnState::Boolean {
                    true_count,
                    false_count,
                    seen_true,
                    seen_false,
                } => {
                    column.true_count = Some(true_count);
                    column.false_count = Some(false_count);
                    column.unique_count = Some((seen_true as u64) + (seen_false as u64));
                }
                ColumnState::Integer(state) => {
                    if let (Some(min), Some(max)) = (state.min, state.max) {
                        column.min_value = Some(ProfileExtreme::Int(min));
                        column.max_value = Some(ProfileExtreme::Int(max));
                        if !state.sum_overflow {
                            column.histogram = Some(integer_histogram(
                                histogram_buckets,
                                min,
                                max,
                                &state.distinct,
                            ));
                        }
                    }
                    if state.sum_overflow {
                        column.sum_overflow = true;
                    } else if non_null_count > 0 {
                        column.sum = Some(ProfileSum::Int(state.sum));
                        column.mean = Some(ProfileMean::Rational(ProfileRational::new(
                            state.sum,
                            non_null_count as u128,
                        )));
                    }
                    column.unique_count = if column.distinct_overflow {
                        None
                    } else {
                        Some(state.distinct.len() as u64)
                    };
                }
                ColumnState::Float(state) => {
                    column.non_finite_count = Some(state.non_finite_count);
                    if let (Some(min), Some(max)) = (state.min, state.max) {
                        column.min_value = Some(ProfileExtreme::Float(ProfileFloat(min)));
                        column.max_value = Some(ProfileExtreme::Float(ProfileFloat(max)));
                        column.histogram = Some(float_histogram(
                            histogram_buckets,
                            min,
                            max,
                            &state.distinct,
                        ));
                    }
                    if state.finite_count > 0 {
                        column.sum = Some(ProfileSum::Float(ProfileFloat(
                            state.exact_sum.finalize_sum(),
                        )));
                        column.mean = Some(ProfileMean::Float(ProfileFloat(
                            state.exact_sum.finalize_mean(state.finite_count),
                        )));
                    }
                    column.unique_count = if column.distinct_overflow {
                        None
                    } else {
                        Some(state.distinct.len() as u64)
                    };
                }
                ColumnState::Date(state) => {
                    if let (Some(min), Some(max)) = (state.min, state.max) {
                        column.min_value = Some(ProfileExtreme::DateDays(min as i32));
                        column.max_value = Some(ProfileExtreme::DateDays(max as i32));
                    }
                    column.unique_count = if column.distinct_overflow {
                        None
                    } else {
                        Some(state.distinct.len() as u64)
                    };
                }
                ColumnState::Timestamp(state) => {
                    if let (Some(min), Some(max)) = (state.min, state.max) {
                        let extreme = |value: i64| match runtime.logical_type {
                            "timestamp_ms" => ProfileExtreme::EpochMs(value),
                            _ => ProfileExtreme::EpochUs(value),
                        };
                        column.min_value = Some(extreme(min));
                        column.max_value = Some(extreme(max));
                    }
                    column.unique_count = if column.distinct_overflow {
                        None
                    } else {
                        Some(state.distinct.len() as u64)
                    };
                }
                ColumnState::Text(state) | ColumnState::Binary(state) => {
                    column.empty_count = Some(state.empty_count);
                    if let Some(min_length) = state.length_min {
                        column.length = Some(ProfileLengthStats {
                            sum_of_lengths: state.length_sum,
                            min_length: Some(min_length),
                            max_length: state.length_max,
                            avg_length: Some(ProfileRational::new(
                                i128::try_from(state.length_sum).unwrap_or(i128::MAX),
                                non_null_count as u128,
                            )),
                            long_value_count: state.long_value_count,
                            histogram: state.length_histogram,
                        });
                    }
                    column.unique_count = if column.distinct_overflow {
                        None
                    } else {
                        Some(state.distinct.len() as u64)
                    };
                    if !column.distinct_overflow {
                        let mut candidates: Vec<(Vec<u8>, u64)> = state
                            .distinct
                            .into_iter()
                            .filter(|(value, _)| value.len() <= PROFILE_MAX_RETAINED_VALUE_BYTES)
                            .collect();
                        candidates.sort_by(|left, right| {
                            right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0))
                        });
                        candidates.truncate(top_k);
                        column.top_values = Some(
                            candidates
                                .into_iter()
                                .map(|(value, count)| match runtime.logical_type {
                                    "binary" => ProfileTopValue::Bytes { value, count },
                                    _ => ProfileTopValue::Text {
                                        value: String::from_utf8(value).unwrap_or_default(),
                                        count,
                                    },
                                })
                                .collect(),
                        );
                    }
                }
            }
            columns.push(column);
        }
        DatasetProfile {
            artifact_type: "profile_report",
            artifact_body_version: 1,
            profiling_contract_version: PROFILING_CONTRACT_VERSION,
            dataset: self.dataset,
            columns,
        }
    }
}

// ---------------------------------------------------------------------------
// Engine integration
// ---------------------------------------------------------------------------

struct ProfileRun {
    dataset: DatasetMetrics,
    columns: Vec<ColumnRuntime>,
    full_row: FullRowState,
}

impl ExecutionEngine {
    /// Q-R1 bounded deterministic streaming profile (ADR-003 §§3–6, §9).
    ///
    /// Reuses the existing Engine run gate and `RequestContext`; there is no
    /// second concurrency mechanism. No partial result escapes a cancelled or
    /// deadline-expired run.
    pub async fn profile(&self, request: ProfileRequest) -> Result<ProfileResult, EngineError> {
        request.context.ensure_active().map_err(map_context_error)?;
        let permit = Arc::clone(&self.run_gate)
            .try_acquire_owned()
            .map_err(|_| EngineError::Busy)?;
        let result = self.profile_inner(request, Uuid::new_v4()).await;
        drop(permit);
        result
    }

    /// Runs Q-R1 over a committed Snapshot while the caller holds the
    /// JobRuntime's single Engine run permit. The Snapshot reader is the only
    /// input path; no SourceAsset inspection or connector read is performed.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn profile_snapshot_with_permit(
        &self,
        store: &SnapshotStore,
        snapshot_id: Uuid,
        columns: ProfileColumns,
        top_k: usize,
        histogram_buckets: usize,
        context: RequestContext,
        run_id: Uuid,
    ) -> Result<ProfileResult, EngineError> {
        context.ensure_active().map_err(map_context_error)?;
        let manifest = store
            .load_manifest(snapshot_id)
            .map_err(EngineError::from_storage)?;
        let schema = manifest.snapshot().schema().clone();
        let reader = store
            .read_batches(snapshot_id)
            .map_err(EngineError::from_storage)?;
        let stream =
            futures::stream::iter(reader).map(|item| item.map_err(EngineError::from_storage));
        profile_batches(
            context,
            schema,
            columns,
            top_k,
            histogram_buckets,
            stream,
            run_id,
        )
        .await
    }

    async fn profile_inner(
        &self,
        request: ProfileRequest,
        run_id: Uuid,
    ) -> Result<ProfileResult, EngineError> {
        let context = request.context.clone();
        context.ensure_active().map_err(map_context_error)?;
        let metadata = self
            .registry
            .inspect(
                &request.connection,
                InspectRequest {
                    context: context.clone(),
                    asset: request.asset.clone(),
                },
            )
            .await
            .map_err(EngineError::from_connector)?;
        context.ensure_active().map_err(map_context_error)?;
        let schema: LogicalSchema = metadata.schema;
        let read = ReadRequest {
            context: context.clone(),
            asset: request.asset.clone(),
            schema_override: Some(schema.clone()),
            projection: None,
            filter: None,
            checkpoint: None,
            batch_size: PROFILE_BATCH_SIZE,
        };
        let stream = self
            .registry
            .read_batches(&request.connection, read)
            .await
            .map_err(EngineError::from_connector)?
            .map(|item| item.map_err(EngineError::from_connector));
        profile_batches(
            context,
            schema,
            request.columns,
            request.top_k,
            request.histogram_buckets,
            stream,
            run_id,
        )
        .await
    }
}

async fn profile_batches<S>(
    context: RequestContext,
    schema: LogicalSchema,
    columns: ProfileColumns,
    top_k: usize,
    histogram_buckets: usize,
    mut stream: S,
    run_id: Uuid,
) -> Result<ProfileResult, EngineError>
where
    S: Stream<Item = Result<BatchEnvelope, EngineError>> + Unpin,
{
    let resolved: Vec<(usize, LogicalType, String)> = match &columns {
        ProfileColumns::All => schema
            .fields
            .iter()
            .enumerate()
            .map(|(index, field)| (index, field.data_type.clone(), field.name.clone()))
            .collect(),
        ProfileColumns::Explicit(names) => {
            let mut resolved = Vec::with_capacity(names.len());
            for name in names {
                let index = schema
                    .fields
                    .iter()
                    .position(|field| field.name == *name)
                    .ok_or(EngineError::InvalidPlan(
                        "profile column not found in schema",
                    ))?;
                let field = &schema.fields[index];
                resolved.push((index, field.data_type.clone(), field.name.clone()));
            }
            resolved
        }
    };
    if resolved.len() > PROFILE_MAX_COLUMNS {
        return Err(EngineError::BoundExceeded(
            "profile resolved column count exceeds PROFILE_MAX_COLUMNS",
        ));
    }

    let mut run = ProfileRun {
        dataset: DatasetMetrics {
            column_count_profiled: resolved.len(),
            ..DatasetMetrics::default()
        },
        columns: resolved
            .iter()
            .map(|(_, logical, name)| ColumnRuntime {
                name: name.clone(),
                logical_type: logical_type_name(logical),
                state: column_state_for(logical),
                null_count: 0,
                distinct_overflow: false,
                state_bytes: 0,
            })
            .collect(),
        full_row: FullRowState {
            keys: BTreeMap::new(),
            bytes: 0,
            overflow: false,
        },
    };

    while let Some(item) = stream.next().await {
        context.ensure_active().map_err(map_context_error)?;
        let envelope = item?;
        // Byte-bound admission: payload bytes are envelope-level facts;
        // an envelope that would push scanned_bytes past the ceiling is
        // not consumed and truncation is disclosed (never an error).
        if run.dataset.scanned_bytes + envelope.byte_count() as u64 > PROFILE_MAX_SCAN_BYTES as u64
        {
            run.dataset.truncated = true;
            break;
        }
        if envelope.schema() != &schema {
            return Err(EngineError::SchemaDrift {
                sequence: envelope.sequence(),
            });
        }
        let payload = envelope.payload();
        let mut rows = payload.num_rows();
        if run.dataset.row_count_scanned + rows as u64 > PROFILE_MAX_ROWS as u64 {
            rows = (PROFILE_MAX_ROWS as u64 - run.dataset.row_count_scanned) as usize;
            run.dataset.truncated = true;
        }
        let view = if rows == payload.num_rows() {
            payload.clone()
        } else {
            payload.slice(0, rows)
        };
        let columns: Vec<(usize, LogicalType)> = resolved
            .iter()
            .map(|(index, logical, _)| (*index, logical.clone()))
            .collect();
        for (runtime_index, (column_index, _)) in columns.iter().enumerate() {
            run.columns[runtime_index].observe(&view, *column_index)?;
        }
        run.full_row.observe(&view, &columns)?;
        run.dataset.row_count_scanned += rows as u64;
        run.dataset.scanned_bytes += envelope.byte_count() as u64;
        if rows < payload.num_rows() {
            break; // row bound reached
        }
    }

    let profile = run.finish(top_k, histogram_buckets);
    let canonical_body = profile.canonical_body();
    let mut hasher = Sha256::new();
    hasher.update(&canonical_body);
    let canonical_digest = hex_lower(&hasher.finalize());
    Ok(ProfileResult {
        run_id,
        profile,
        canonical_body,
        canonical_digest,
    })
}
