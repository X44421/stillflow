use stillflow_core::{LogicalType, TimeUnit};

pub(crate) fn fixed_slot_bytes(data_type: &LogicalType) -> Option<usize> {
    match data_type {
        LogicalType::Null => Some(0),
        LogicalType::Boolean | LogicalType::Int8 | LogicalType::UInt8 => Some(1),
        LogicalType::Int16 | LogicalType::UInt16 => Some(2),
        LogicalType::Int32 | LogicalType::UInt32 | LogicalType::Float32 | LogicalType::Date32 => {
            Some(4)
        }
        LogicalType::Int64
        | LogicalType::UInt64
        | LogicalType::Float64
        | LogicalType::Timestamp {
            unit:
                TimeUnit::Second | TimeUnit::Millisecond | TimeUnit::Microsecond | TimeUnit::Nanosecond,
            ..
        } => Some(8),
        LogicalType::Utf8 | LogicalType::Binary | LogicalType::List(_) | LogicalType::Struct(_) => {
            None
        }
    }
}

pub(crate) fn polars_data_type(
    data_type: &LogicalType,
) -> Result<polars::prelude::DataType, crate::error::EngineError> {
    use polars::prelude::{DataType, TimeUnit as PolarsTimeUnit, TimeZone};
    Ok(match data_type {
        LogicalType::Null => DataType::Null,
        LogicalType::Boolean => DataType::Boolean,
        LogicalType::Int8 => DataType::Int8,
        LogicalType::Int16 => DataType::Int16,
        LogicalType::Int32 => DataType::Int32,
        LogicalType::Int64 => DataType::Int64,
        LogicalType::UInt8 => DataType::UInt8,
        LogicalType::UInt16 => DataType::UInt16,
        LogicalType::UInt32 => DataType::UInt32,
        LogicalType::UInt64 => DataType::UInt64,
        LogicalType::Float32 => DataType::Float32,
        LogicalType::Float64 => DataType::Float64,
        LogicalType::Utf8 => DataType::String,
        LogicalType::Binary => DataType::Binary,
        LogicalType::Date32 => DataType::Date,
        LogicalType::Timestamp { unit, timezone } => DataType::Datetime(
            match unit {
                TimeUnit::Millisecond => PolarsTimeUnit::Milliseconds,
                TimeUnit::Microsecond => PolarsTimeUnit::Microseconds,
                TimeUnit::Nanosecond => PolarsTimeUnit::Nanoseconds,
                TimeUnit::Second => {
                    return Err(crate::error::EngineError::TypeError(
                        "timestamp second unit is paused",
                    ));
                }
            },
            timezone.clone().map(TimeZone::from_string),
        ),
        LogicalType::List(_) | LogicalType::Struct(_) => {
            return Err(crate::error::EngineError::TypeError(
                "nested list and struct lowering is limited to passthrough projection",
            ));
        }
    })
}
