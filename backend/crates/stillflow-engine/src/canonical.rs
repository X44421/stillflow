//! Canonical exact-dedup key encoding for the E4 experimental slice.

use polars::prelude::{AnyValue, DataFrame, TimeUnit as PolarsTimeUnit};
use stillflow_core::{ColumnId, LogicalSchema, LogicalType, TimeUnit};

use crate::error::EngineError;
use crate::MAX_DEDUP_KEY_BYTES;

const CANONICAL_QNAN_F32: u32 = 0x7FC0_0000;
const CANONICAL_QNAN_F64: u64 = 0x7FF8_0000_0000_0000;

pub(crate) fn canonical_key_bytes(
    frame: &DataFrame,
    row: usize,
    schema: &LogicalSchema,
    keys: &[ColumnId],
) -> Result<Vec<u8>, EngineError> {
    let mut out = Vec::new();
    for key in keys {
        let field = schema.field(*key).ok_or(EngineError::UnknownColumn(*key))?;
        let column = frame
            .column(field.name.as_str())
            .map_err(|_| EngineError::UnknownColumn(*key))?;
        let value = column
            .get(row)
            .map_err(|_| EngineError::Internal("dedup key row is out of range"))?;
        encode_component(&mut out, &field.data_type, &value)?;
        if out.len() > MAX_DEDUP_KEY_BYTES {
            return Err(EngineError::BoundExceeded(
                "encoded dedup key exceeds MAX_DEDUP_KEY_BYTES",
            ));
        }
    }
    if out.len() > MAX_DEDUP_KEY_BYTES {
        return Err(EngineError::BoundExceeded(
            "encoded dedup key exceeds MAX_DEDUP_KEY_BYTES",
        ));
    }
    Ok(out)
}

fn encode_component(
    out: &mut Vec<u8>,
    declared: &LogicalType,
    value: &AnyValue<'_>,
) -> Result<(), EngineError> {
    if matches!(value, AnyValue::Null) {
        out.push(0x00);
        return Ok(());
    }
    match declared {
        LogicalType::Boolean => {
            out.push(0x01);
            out.push(u8::from(as_bool(value)?));
        }
        LogicalType::Int8 => {
            out.push(0x02);
            out.extend_from_slice(
                &i8::try_from(as_i64(value)?)
                    .map_err(|_| int_range())?
                    .to_le_bytes(),
            );
        }
        LogicalType::Int16 => {
            out.push(0x03);
            out.extend_from_slice(
                &i16::try_from(as_i64(value)?)
                    .map_err(|_| int_range())?
                    .to_le_bytes(),
            );
        }
        LogicalType::Int32 => {
            out.push(0x04);
            out.extend_from_slice(
                &i32::try_from(as_i64(value)?)
                    .map_err(|_| int_range())?
                    .to_le_bytes(),
            );
        }
        LogicalType::Int64 => {
            out.push(0x05);
            out.extend_from_slice(&as_i64(value)?.to_le_bytes());
        }
        LogicalType::UInt8 => {
            out.push(0x06);
            out.push(u8::try_from(as_u64(value)?).map_err(|_| int_range())?);
        }
        LogicalType::UInt16 => {
            out.push(0x07);
            out.extend_from_slice(
                &u16::try_from(as_u64(value)?)
                    .map_err(|_| int_range())?
                    .to_le_bytes(),
            );
        }
        LogicalType::UInt32 => {
            out.push(0x08);
            out.extend_from_slice(
                &u32::try_from(as_u64(value)?)
                    .map_err(|_| int_range())?
                    .to_le_bytes(),
            );
        }
        LogicalType::UInt64 => {
            out.push(0x09);
            out.extend_from_slice(&as_u64(value)?.to_le_bytes());
        }
        LogicalType::Float32 => {
            out.push(0x0A);
            out.extend_from_slice(&canonical_f32(as_f32(value)?).to_le_bytes());
        }
        LogicalType::Float64 => {
            out.push(0x0B);
            out.extend_from_slice(&canonical_f64(as_f64(value)?).to_le_bytes());
        }
        LogicalType::Utf8 => {
            out.push(0x0C);
            let bytes = as_utf8(value)?.as_bytes();
            let len = u32::try_from(bytes.len())
                .map_err(|_| EngineError::BoundExceeded("utf8 key component is too large"))?;
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(bytes);
        }
        LogicalType::Binary => {
            out.push(0x0D);
            let bytes = as_binary(value)?;
            let len = u32::try_from(bytes.len())
                .map_err(|_| EngineError::BoundExceeded("binary key component is too large"))?;
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(bytes);
        }
        LogicalType::Date32 => {
            out.push(0x0E);
            out.extend_from_slice(&as_date32(value)?.to_le_bytes());
        }
        LogicalType::Timestamp { unit, timezone } => {
            if matches!(unit, TimeUnit::Second) {
                return Err(EngineError::TypeError(
                    "timestamp second unit is paused for dedup keys",
                ));
            }
            out.push(0x0F);
            out.push(match unit {
                TimeUnit::Millisecond => 1,
                TimeUnit::Microsecond => 2,
                TimeUnit::Nanosecond => 3,
                TimeUnit::Second => 0,
            });
            match timezone {
                None => out.push(0),
                Some(zone) => {
                    out.push(1);
                    let bytes = zone.as_bytes();
                    let len = u32::try_from(bytes.len()).map_err(|_| {
                        EngineError::BoundExceeded("timestamp timezone is too large")
                    })?;
                    out.extend_from_slice(&len.to_le_bytes());
                    out.extend_from_slice(bytes);
                }
            }
            out.extend_from_slice(&as_timestamp(value, *unit)?.to_le_bytes());
        }
        LogicalType::Null => {
            out.push(0x00);
        }
        LogicalType::List(_) | LogicalType::Struct(_) => {
            return Err(EngineError::TypeError(
                "list and struct dedup keys are paused",
            ));
        }
    }
    Ok(())
}

fn canonical_f32(value: f32) -> u32 {
    if value.is_nan() {
        CANONICAL_QNAN_F32
    } else if value == 0.0 {
        0
    } else {
        value.to_bits()
    }
}

fn canonical_f64(value: f64) -> u64 {
    if value.is_nan() {
        CANONICAL_QNAN_F64
    } else if value == 0.0 {
        0
    } else {
        value.to_bits()
    }
}

fn as_bool(value: &AnyValue<'_>) -> Result<bool, EngineError> {
    match value {
        AnyValue::Boolean(value) => Ok(*value),
        _ => Err(EngineError::TypeError("dedup key is not boolean")),
    }
}

fn as_i64(value: &AnyValue<'_>) -> Result<i64, EngineError> {
    match *value {
        AnyValue::Int8(value) => Ok(i64::from(value)),
        AnyValue::Int16(value) => Ok(i64::from(value)),
        AnyValue::Int32(value) => Ok(i64::from(value)),
        AnyValue::Int64(value) => Ok(value),
        _ => Err(EngineError::TypeError("dedup key is not a signed integer")),
    }
}

fn as_u64(value: &AnyValue<'_>) -> Result<u64, EngineError> {
    match *value {
        AnyValue::UInt8(value) => Ok(u64::from(value)),
        AnyValue::UInt16(value) => Ok(u64::from(value)),
        AnyValue::UInt32(value) => Ok(u64::from(value)),
        AnyValue::UInt64(value) => Ok(value),
        _ => Err(EngineError::TypeError(
            "dedup key is not an unsigned integer",
        )),
    }
}

fn as_f32(value: &AnyValue<'_>) -> Result<f32, EngineError> {
    match *value {
        AnyValue::Float32(value) => Ok(value),
        AnyValue::Float64(value) => Ok(value as f32),
        _ => Err(EngineError::TypeError("dedup key is not float32")),
    }
}

fn as_f64(value: &AnyValue<'_>) -> Result<f64, EngineError> {
    match *value {
        AnyValue::Float64(value) => Ok(value),
        AnyValue::Float32(value) => Ok(f64::from(value)),
        _ => Err(EngineError::TypeError("dedup key is not float64")),
    }
}

fn as_utf8<'a>(value: &'a AnyValue<'_>) -> Result<&'a str, EngineError> {
    match value {
        AnyValue::String(value) => Ok(*value),
        AnyValue::StringOwned(value) => Ok(value.as_str()),
        _ => Err(EngineError::TypeError("dedup key is not utf8")),
    }
}

fn as_binary<'a>(value: &'a AnyValue<'_>) -> Result<&'a [u8], EngineError> {
    match value {
        AnyValue::Binary(value) => Ok(*value),
        AnyValue::BinaryOwned(value) => Ok(value.as_slice()),
        _ => Err(EngineError::TypeError("dedup key is not binary")),
    }
}

fn as_date32(value: &AnyValue<'_>) -> Result<i32, EngineError> {
    match *value {
        AnyValue::Date(value) => Ok(value),
        AnyValue::Int32(value) => Ok(value),
        _ => Err(EngineError::TypeError("dedup key is not date32")),
    }
}

fn as_timestamp(value: &AnyValue<'_>, unit: TimeUnit) -> Result<i64, EngineError> {
    match *value {
        AnyValue::Datetime(value, polars_unit, _) => {
            if polars_unit_matches(polars_unit, unit) {
                Ok(value)
            } else {
                Err(EngineError::TypeError("dedup timestamp unit mismatch"))
            }
        }
        AnyValue::Int64(value) => Ok(value),
        _ => Err(EngineError::TypeError("dedup key is not a timestamp")),
    }
}

fn polars_unit_matches(polars_unit: PolarsTimeUnit, unit: TimeUnit) -> bool {
    matches!(
        (polars_unit, unit),
        (PolarsTimeUnit::Milliseconds, TimeUnit::Millisecond)
            | (PolarsTimeUnit::Microseconds, TimeUnit::Microsecond)
            | (PolarsTimeUnit::Nanoseconds, TimeUnit::Nanosecond)
    )
}

fn int_range() -> EngineError {
    EngineError::TypeError("dedup integer key is outside the declared width")
}
