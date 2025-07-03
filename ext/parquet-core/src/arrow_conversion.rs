//! Bidirectional conversion between Arrow arrays and ParquetValue
//!
//! This module provides a unified interface for converting between Arrow's
//! columnar format and Parquet's value representation. It consolidates
//! the conversion logic that was previously duplicated between the reader
//! and writer modules.

use crate::{ParquetError, ParquetValue, Result};
use arrow_array::{builder::*, Array, ArrayRef, ListArray, MapArray, StructArray};
use arrow_schema::{DataType, Field};
use bytes::Bytes;
use indexmap::IndexMap;
use ordered_float::OrderedFloat;
use std::sync::Arc;

/// Convert a single value from an Arrow array at the given index to a ParquetValue
pub fn arrow_to_parquet_value(array: &dyn Array, index: usize) -> Result<ParquetValue> {
    use arrow_array::*;

    if array.is_null(index) {
        return Ok(ParquetValue::Null);
    }

    match array.data_type() {
        // Primitive types
        DataType::Boolean => {
            let array = downcast_array::<BooleanArray>(array)?;
            Ok(ParquetValue::Boolean(array.value(index)))
        }
        DataType::Int8 => {
            let array = downcast_array::<Int8Array>(array)?;
            Ok(ParquetValue::Int8(array.value(index)))
        }
        DataType::Int16 => {
            let array = downcast_array::<Int16Array>(array)?;
            Ok(ParquetValue::Int16(array.value(index)))
        }
        DataType::Int32 => {
            let array = downcast_array::<Int32Array>(array)?;
            Ok(ParquetValue::Int32(array.value(index)))
        }
        DataType::Int64 => {
            let array = downcast_array::<Int64Array>(array)?;
            Ok(ParquetValue::Int64(array.value(index)))
        }
        DataType::UInt8 => {
            let array = downcast_array::<UInt8Array>(array)?;
            Ok(ParquetValue::UInt8(array.value(index)))
        }
        DataType::UInt16 => {
            let array = downcast_array::<UInt16Array>(array)?;
            Ok(ParquetValue::UInt16(array.value(index)))
        }
        DataType::UInt32 => {
            let array = downcast_array::<UInt32Array>(array)?;
            Ok(ParquetValue::UInt32(array.value(index)))
        }
        DataType::UInt64 => {
            let array = downcast_array::<UInt64Array>(array)?;
            Ok(ParquetValue::UInt64(array.value(index)))
        }
        DataType::Float16 => {
            let array = downcast_array::<Float16Array>(array)?;
            let value = array.value(index);
            Ok(ParquetValue::Float16(OrderedFloat(value.to_f32())))
        }
        DataType::Float32 => {
            let array = downcast_array::<Float32Array>(array)?;
            Ok(ParquetValue::Float32(OrderedFloat(array.value(index))))
        }
        DataType::Float64 => {
            let array = downcast_array::<Float64Array>(array)?;
            Ok(ParquetValue::Float64(OrderedFloat(array.value(index))))
        }

        // String and binary types
        DataType::Utf8 => {
            let array = downcast_array::<StringArray>(array)?;
            Ok(ParquetValue::String(Arc::from(array.value(index))))
        }
        DataType::Binary => {
            let array = downcast_array::<BinaryArray>(array)?;
            Ok(ParquetValue::Bytes(Bytes::copy_from_slice(
                array.value(index),
            )))
        }
        DataType::FixedSizeBinary(_) => {
            let array = downcast_array::<FixedSizeBinaryArray>(array)?;
            Ok(ParquetValue::Bytes(Bytes::copy_from_slice(
                array.value(index),
            )))
        }

        // Date and time types
        DataType::Date32 => {
            let array = downcast_array::<Date32Array>(array)?;
            Ok(ParquetValue::Date32(array.value(index)))
        }
        DataType::Date64 => {
            let array = downcast_array::<Date64Array>(array)?;
            Ok(ParquetValue::Date64(array.value(index)))
        }

        // Timestamp types
        DataType::Timestamp(unit, timezone) => {
            let timezone = timezone.as_ref().map(|s| Arc::from(s.as_ref()));
            match unit {
                arrow_schema::TimeUnit::Millisecond => {
                    let array = downcast_array::<TimestampMillisecondArray>(array)?;
                    Ok(ParquetValue::TimestampMillis(array.value(index), timezone))
                }
                arrow_schema::TimeUnit::Microsecond => {
                    let array = downcast_array::<TimestampMicrosecondArray>(array)?;
                    Ok(ParquetValue::TimestampMicros(array.value(index), timezone))
                }
                arrow_schema::TimeUnit::Second => {
                    let array = downcast_array::<TimestampSecondArray>(array)?;
                    Ok(ParquetValue::TimestampSecond(array.value(index), timezone))
                }
                arrow_schema::TimeUnit::Nanosecond => {
                    let array = downcast_array::<TimestampNanosecondArray>(array)?;
                    Ok(ParquetValue::TimestampNanos(array.value(index), timezone))
                }
            }
        }

        // Time types
        DataType::Time32(unit) => match unit {
            arrow_schema::TimeUnit::Millisecond => {
                let array = downcast_array::<Time32MillisecondArray>(array)?;
                Ok(ParquetValue::TimeMillis(array.value(index)))
            }
            _ => Err(ParquetError::Conversion(format!(
                "Unsupported time32 unit: {:?}",
                unit
            ))),
        },
        DataType::Time64(unit) => match unit {
            arrow_schema::TimeUnit::Microsecond => {
                let array = downcast_array::<Time64MicrosecondArray>(array)?;
                Ok(ParquetValue::TimeMicros(array.value(index)))
            }
            _ => Err(ParquetError::Conversion(format!(
                "Unsupported time64 unit: {:?}",
                unit
            ))),
        },

        // Decimal types
        DataType::Decimal128(_precision, scale) => {
            let array = downcast_array::<Decimal128Array>(array)?;
            let value = array.value(index);
            Ok(ParquetValue::Decimal128(value, *scale))
        }
        DataType::Decimal256(_precision, scale) => {
            let array = downcast_array::<Decimal256Array>(array)?;
            let bytes = array.value(index).to_le_bytes();

            // Convert to BigInt
            let bigint = if bytes[31] & 0x80 != 0 {
                // Negative number - convert from two's complement
                let mut inverted = [0u8; 32];
                for (i, &b) in bytes.iter().enumerate() {
                    inverted[i] = !b;
                }
                let positive = num::BigInt::from_bytes_le(num::bigint::Sign::Plus, &inverted);
                -(positive + num::BigInt::from(1))
            } else {
                num::BigInt::from_bytes_le(num::bigint::Sign::Plus, &bytes)
            };

            Ok(ParquetValue::Decimal256(bigint, *scale))
        }

        // Complex types
        DataType::List(_) => {
            let array = downcast_array::<ListArray>(array)?;
            let list_values = array.value(index);

            let mut values = Vec::with_capacity(list_values.len());
            for i in 0..list_values.len() {
                values.push(arrow_to_parquet_value(&list_values, i)?);
            }

            Ok(ParquetValue::List(values))
        }
        DataType::Map(_, _) => {
            let array = downcast_array::<MapArray>(array)?;
            let map_value = array.value(index);

            // Map is stored as a struct with two fields: keys and values
            let keys = map_value.column(0);
            let values = map_value.column(1);

            let mut map_vec = Vec::with_capacity(keys.len());
            for i in 0..keys.len() {
                let key = arrow_to_parquet_value(keys, i)?;
                let value = arrow_to_parquet_value(values, i)?;
                map_vec.push((key, value));
            }

            Ok(ParquetValue::Map(map_vec))
        }
        DataType::Struct(_) => {
            let array = downcast_array::<StructArray>(array)?;

            let mut map = IndexMap::new();
            for (col_idx, field) in array.fields().iter().enumerate() {
                let column = array.column(col_idx);
                let value = arrow_to_parquet_value(column, index)?;
                map.insert(Arc::from(field.name().as_str()), value);
            }

            Ok(ParquetValue::Record(map))
        }

        dt => Err(ParquetError::Conversion(format!(
            "Unsupported data type for conversion: {:?}",
            dt
        ))),
    }
}

/// Convert a vector of ParquetValues to an Arrow array
pub fn parquet_values_to_arrow_array(values: Vec<ParquetValue>, field: &Field) -> Result<ArrayRef> {
    match field.data_type() {
        // Boolean
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(values.len());
            for value in values {
                match value {
                    ParquetValue::Boolean(b) => builder.append_value(b),
                    ParquetValue::Null => builder.append_null(),
                    _ => {
                        return Err(ParquetError::Conversion(format!(
                            "Expected Boolean, got {:?}",
                            value.type_name()
                        )))
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }

        // Integer types with automatic upcasting
        DataType::Int8 => build_int8_array(values),
        DataType::Int16 => build_int16_array(values),
        DataType::Int32 => build_int32_array(values),
        DataType::Int64 => build_int64_array(values),
        DataType::UInt8 => build_uint8_array(values),
        DataType::UInt16 => build_uint16_array(values),
        DataType::UInt32 => build_uint32_array(values),
        DataType::UInt64 => build_uint64_array(values),

        // Float types
        DataType::Float32 => build_float32_array(values),
        DataType::Float64 => build_float64_array(values),

        // String and binary
        DataType::Utf8 => build_string_array(values),
        DataType::Binary => build_binary_array(values),
        DataType::FixedSizeBinary(size) => build_fixed_binary_array(values, *size),

        // Date and time
        DataType::Date32 => build_date32_array(values),
        DataType::Date64 => build_date64_array(values),
        DataType::Time32(unit) => build_time32_array(values, unit),
        DataType::Time64(unit) => build_time64_array(values, unit),

        // Timestamp
        DataType::Timestamp(unit, tz) => build_timestamp_array(values, unit, tz.as_deref()),

        // Decimal
        DataType::Decimal128(precision, scale) => {
            build_decimal128_array(values, *precision, *scale)
        }
        DataType::Decimal256(precision, scale) => {
            build_decimal256_array(values, *precision, *scale)
        }

        // Complex types
        DataType::List(item_field) => build_list_array(values, item_field),
        DataType::Map(entries_field, sorted) => build_map_array(values, entries_field, *sorted),
        DataType::Struct(fields) => build_struct_array(values, fields),

        dt => Err(ParquetError::Conversion(format!(
            "Unsupported data type for conversion: {:?}",
            dt
        ))),
    }
}

/// Helper function to downcast an array with better error messages
fn downcast_array<T: 'static>(array: &dyn Array) -> Result<&T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        ParquetError::Conversion(format!("Failed to cast to {}", std::any::type_name::<T>()))
    })
}

/// Build Int8 array
fn build_int8_array(values: Vec<ParquetValue>) -> Result<ArrayRef> {
    let mut builder = Int8Builder::with_capacity(values.len());
    for value in values {
        match value {
            ParquetValue::Int8(i) => builder.append_value(i),
            ParquetValue::Null => builder.append_null(),
            _ => {
                return Err(ParquetError::Conversion(format!(
                    "Expected Int8, got {:?}",
                    value.type_name()
                )))
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Build Int16 array
fn build_int16_array(values: Vec<ParquetValue>) -> Result<ArrayRef> {
    let mut builder = Int16Builder::with_capacity(values.len());
    for value in values {
        match value {
            ParquetValue::Int16(i) => builder.append_value(i),
            ParquetValue::Int8(i) => builder.append_value(i as i16),
            ParquetValue::Null => builder.append_null(),
            _ => {
                return Err(ParquetError::Conversion(format!(
                    "Expected Int16, got {:?}",
                    value.type_name()
                )))
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Build Int32 array
fn build_int32_array(values: Vec<ParquetValue>) -> Result<ArrayRef> {
    let mut builder = Int32Builder::with_capacity(values.len());
    for value in values {
        match value {
            ParquetValue::Int32(i) => builder.append_value(i),
            ParquetValue::Int16(i) => builder.append_value(i as i32),
            ParquetValue::Int8(i) => builder.append_value(i as i32),
            ParquetValue::Null => builder.append_null(),
            _ => {
                return Err(ParquetError::Conversion(format!(
                    "Expected Int32, got {:?}",
                    value.type_name()
                )))
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Build Int64 array
fn build_int64_array(values: Vec<ParquetValue>) -> Result<ArrayRef> {
    let mut builder = Int64Builder::with_capacity(values.len());
    for value in values {
        match value {
            ParquetValue::Int64(i) => builder.append_value(i),
            ParquetValue::Int32(i) => builder.append_value(i as i64),
            ParquetValue::Int16(i) => builder.append_value(i as i64),
            ParquetValue::Int8(i) => builder.append_value(i as i64),
            ParquetValue::Null => builder.append_null(),
            _ => {
                return Err(ParquetError::Conversion(format!(
                    "Expected Int64, got {:?}",
                    value.type_name()
                )))
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Build UInt8 array
fn build_uint8_array(values: Vec<ParquetValue>) -> Result<ArrayRef> {
    let mut builder = UInt8Builder::with_capacity(values.len());
    for value in values {
        match value {
            ParquetValue::UInt8(i) => builder.append_value(i),
            ParquetValue::Null => builder.append_null(),
            _ => {
                return Err(ParquetError::Conversion(format!(
                    "Expected UInt8, got {:?}",
                    value.type_name()
                )))
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Build UInt16 array
fn build_uint16_array(values: Vec<ParquetValue>) -> Result<ArrayRef> {
    let mut builder = UInt16Builder::with_capacity(values.len());
    for value in values {
        match value {
            ParquetValue::UInt16(i) => builder.append_value(i),
            ParquetValue::UInt8(i) => builder.append_value(i as u16),
            ParquetValue::Null => builder.append_null(),
            _ => {
                return Err(ParquetError::Conversion(format!(
                    "Expected UInt16, got {:?}",
                    value.type_name()
                )))
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Build UInt32 array
fn build_uint32_array(values: Vec<ParquetValue>) -> Result<ArrayRef> {
    let mut builder = UInt32Builder::with_capacity(values.len());
    for value in values {
        match value {
            ParquetValue::UInt32(i) => builder.append_value(i),
            ParquetValue::UInt16(i) => builder.append_value(i as u32),
            ParquetValue::UInt8(i) => builder.append_value(i as u32),
            ParquetValue::Null => builder.append_null(),
            _ => {
                return Err(ParquetError::Conversion(format!(
                    "Expected UInt32, got {:?}",
                    value.type_name()
                )))
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Build UInt64 array
fn build_uint64_array(values: Vec<ParquetValue>) -> Result<ArrayRef> {
    let mut builder = UInt64Builder::with_capacity(values.len());
    for value in values {
        match value {
            ParquetValue::UInt64(i) => builder.append_value(i),
            ParquetValue::UInt32(i) => builder.append_value(i as u64),
            ParquetValue::UInt16(i) => builder.append_value(i as u64),
            ParquetValue::UInt8(i) => builder.append_value(i as u64),
            ParquetValue::Null => builder.append_null(),
            _ => {
                return Err(ParquetError::Conversion(format!(
                    "Expected UInt64, got {:?}",
                    value.type_name()
                )))
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Build Float32 array with Float16 support
fn build_float32_array(values: Vec<ParquetValue>) -> Result<ArrayRef> {
    let mut builder = Float32Builder::with_capacity(values.len());
    for value in values {
        match value {
            ParquetValue::Float32(OrderedFloat(f)) => builder.append_value(f),
            ParquetValue::Float16(OrderedFloat(f)) => builder.append_value(f),
            ParquetValue::Null => builder.append_null(),
            _ => {
                return Err(ParquetError::Conversion(format!(
                    "Expected Float32, got {:?}",
                    value.type_name()
                )))
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Build Float64 array with Float32 and Float16 support
fn build_float64_array(values: Vec<ParquetValue>) -> Result<ArrayRef> {
    let mut builder = Float64Builder::with_capacity(values.len());
    for value in values {
        match value {
            ParquetValue::Float64(OrderedFloat(f)) => builder.append_value(f),
            ParquetValue::Float32(OrderedFloat(f)) => builder.append_value(f as f64),
            ParquetValue::Float16(OrderedFloat(f)) => builder.append_value(f as f64),
            ParquetValue::Null => builder.append_null(),
            _ => {
                return Err(ParquetError::Conversion(format!(
                    "Expected Float64, got {:?}",
                    value.type_name()
                )))
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Build string array
fn build_string_array(values: Vec<ParquetValue>) -> Result<ArrayRef> {
    let mut builder = StringBuilder::with_capacity(values.len(), 0);
    for value in values {
        match value {
            ParquetValue::String(s) => builder.append_value(&s),
            ParquetValue::Null => builder.append_null(),
            _ => {
                return Err(ParquetError::Conversion(format!(
                    "Expected String, got {:?}",
                    value.type_name()
                )))
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Build binary array
fn build_binary_array(values: Vec<ParquetValue>) -> Result<ArrayRef> {
    let mut builder = BinaryBuilder::with_capacity(values.len(), 0);
    for value in values {
        match value {
            ParquetValue::Bytes(b) => builder.append_value(&b),
            ParquetValue::Null => builder.append_null(),
            _ => {
                return Err(ParquetError::Conversion(format!(
                    "Expected Bytes, got {:?}",
                    value.type_name()
                )))
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Build fixed size binary array
fn build_fixed_binary_array(values: Vec<ParquetValue>, size: i32) -> Result<ArrayRef> {
    let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), size);
    for value in values {
        match value {
            ParquetValue::Bytes(b) => {
                if b.len() != size as usize {
                    return Err(ParquetError::Conversion(format!(
                        "Fixed size binary expected {} bytes, got {}",
                        size,
                        b.len()
                    )));
                }
                builder.append_value(&b)?;
            }
            ParquetValue::Null => builder.append_null(),
            _ => {
                return Err(ParquetError::Conversion(format!(
                    "Expected Bytes, got {:?}",
                    value.type_name()
                )))
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Build Date32 array
fn build_date32_array(values: Vec<ParquetValue>) -> Result<ArrayRef> {
    let mut builder = Date32Builder::with_capacity(values.len());
    for value in values {
        match value {
            ParquetValue::Date32(d) => builder.append_value(d),
            ParquetValue::Null => builder.append_null(),
            _ => {
                return Err(ParquetError::Conversion(format!(
                    "Expected Date32, got {:?}",
                    value.type_name()
                )))
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Build Date64 array
fn build_date64_array(values: Vec<ParquetValue>) -> Result<ArrayRef> {
    let mut builder = Date64Builder::with_capacity(values.len());
    for value in values {
        match value {
            ParquetValue::Date64(d) => builder.append_value(d),
            ParquetValue::Null => builder.append_null(),
            _ => {
                return Err(ParquetError::Conversion(format!(
                    "Expected Date64, got {:?}",
                    value.type_name()
                )))
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Build Time32 array
fn build_time32_array(
    values: Vec<ParquetValue>,
    unit: &arrow_schema::TimeUnit,
) -> Result<ArrayRef> {
    match unit {
        arrow_schema::TimeUnit::Millisecond => {
            let mut builder = Time32MillisecondBuilder::with_capacity(values.len());
            for value in values {
                match value {
                    ParquetValue::TimeMillis(t) => builder.append_value(t),
                    ParquetValue::Null => builder.append_null(),
                    _ => {
                        return Err(ParquetError::Conversion(format!(
                            "Expected TimeMillis, got {:?}",
                            value.type_name()
                        )))
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => Err(ParquetError::Conversion(format!(
            "Unsupported time32 unit: {:?}",
            unit
        ))),
    }
}

/// Build Time64 array
fn build_time64_array(
    values: Vec<ParquetValue>,
    unit: &arrow_schema::TimeUnit,
) -> Result<ArrayRef> {
    match unit {
        arrow_schema::TimeUnit::Microsecond => {
            let mut builder = Time64MicrosecondBuilder::with_capacity(values.len());
            for value in values {
                match value {
                    ParquetValue::TimeMicros(t) => builder.append_value(t),
                    ParquetValue::Null => builder.append_null(),
                    _ => {
                        return Err(ParquetError::Conversion(format!(
                            "Expected TimeMicros, got {:?}",
                            value.type_name()
                        )))
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => Err(ParquetError::Conversion(format!(
            "Unsupported time64 unit: {:?}",
            unit
        ))),
    }
}

/// Build timestamp array
fn build_timestamp_array(
    values: Vec<ParquetValue>,
    unit: &arrow_schema::TimeUnit,
    timezone: Option<&str>,
) -> Result<ArrayRef> {
    // First, check if all values have the same timezone (or use the field timezone)
    let mut common_tz: Option<Option<Arc<str>>> = None;
    for value in &values {
        match value {
            ParquetValue::TimestampSecond(_, tz)
            | ParquetValue::TimestampMillis(_, tz)
            | ParquetValue::TimestampMicros(_, tz)
            | ParquetValue::TimestampNanos(_, tz) => {
                match &common_tz {
                    None => common_tz = Some(tz.clone()),
                    Some(existing) => {
                        // If we have mixed timezones, we'll use the field timezone
                        if existing != tz {
                            common_tz = Some(timezone.map(Arc::from));
                            break;
                        }
                    }
                }
            }
            ParquetValue::Null => {}
            _ => {}
        }
    }

    // Use the common timezone from values, or fall back to field timezone
    let tz = common_tz.unwrap_or_else(|| timezone.map(Arc::from));

    match unit {
        arrow_schema::TimeUnit::Second => {
            let mut builder =
                TimestampSecondBuilder::with_capacity(values.len()).with_timezone_opt(tz.clone());
            for value in values {
                match value {
                    ParquetValue::TimestampSecond(t, _) => builder.append_value(t),
                    ParquetValue::Null => builder.append_null(),
                    _ => {
                        return Err(ParquetError::Conversion(format!(
                            "Expected TimestampSecond, got {:?}",
                            value.type_name()
                        )))
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        arrow_schema::TimeUnit::Millisecond => {
            let mut builder = TimestampMillisecondBuilder::with_capacity(values.len())
                .with_timezone_opt(tz.clone());
            for value in values {
                match value {
                    ParquetValue::TimestampMillis(t, _) => builder.append_value(t),
                    ParquetValue::Null => builder.append_null(),
                    _ => {
                        return Err(ParquetError::Conversion(format!(
                            "Expected TimestampMillis, got {:?}",
                            value.type_name()
                        )))
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        arrow_schema::TimeUnit::Microsecond => {
            let mut builder = TimestampMicrosecondBuilder::with_capacity(values.len())
                .with_timezone_opt(tz.clone());
            for value in values {
                match value {
                    ParquetValue::TimestampMicros(t, _) => builder.append_value(t),
                    ParquetValue::Null => builder.append_null(),
                    _ => {
                        return Err(ParquetError::Conversion(format!(
                            "Expected TimestampMicros, got {:?}",
                            value.type_name()
                        )))
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        arrow_schema::TimeUnit::Nanosecond => {
            let mut builder = TimestampNanosecondBuilder::with_capacity(values.len())
                .with_timezone_opt(tz.clone());
            for value in values {
                match value {
                    ParquetValue::TimestampNanos(t, _) => builder.append_value(t),
                    ParquetValue::Null => builder.append_null(),
                    _ => {
                        return Err(ParquetError::Conversion(format!(
                            "Expected TimestampNanos, got {:?}",
                            value.type_name()
                        )))
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
    }
}

/// Build Decimal128 array
fn build_decimal128_array(values: Vec<ParquetValue>, precision: u8, scale: i8) -> Result<ArrayRef> {
    let mut builder = Decimal128Builder::with_capacity(values.len())
        .with_precision_and_scale(precision, scale)?;
    for value in values {
        match value {
            ParquetValue::Decimal128(d, _) => builder.append_value(d),
            ParquetValue::Null => builder.append_null(),
            _ => {
                return Err(ParquetError::Conversion(format!(
                    "Expected Decimal128, got {:?}",
                    value.type_name()
                )))
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Build Decimal256 array
fn build_decimal256_array(values: Vec<ParquetValue>, precision: u8, scale: i8) -> Result<ArrayRef> {
    let mut builder = Decimal256Builder::with_capacity(values.len())
        .with_precision_and_scale(precision, scale)?;
    for value in values {
        match value {
            ParquetValue::Decimal256(bigint, _) => {
                let bytes = decimal256_from_bigint(&bigint)?;
                builder.append_value(bytes);
            }
            ParquetValue::Null => builder.append_null(),
            _ => {
                return Err(ParquetError::Conversion(format!(
                    "Expected Decimal256, got {:?}",
                    value.type_name()
                )))
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Convert BigInt to i256 (32-byte array)
fn decimal256_from_bigint(bigint: &num::BigInt) -> Result<arrow_buffer::i256> {
    // Get bytes in little-endian format
    let (sign, mut bytes) = bigint.to_bytes_le();

    // Ensure we have exactly 32 bytes
    if bytes.len() > 32 {
        return Err(ParquetError::Conversion(
            "Decimal256 value too large".to_string(),
        ));
    }

    // Pad with zeros or ones (for negative numbers) to reach 32 bytes
    bytes.resize(32, 0);

    // If negative, convert to two's complement
    if sign == num::bigint::Sign::Minus {
        // Invert all bits
        for byte in &mut bytes {
            *byte = !*byte;
        }
        // Add 1
        let mut carry = true;
        for byte in &mut bytes {
            if carry {
                let (new_byte, new_carry) = byte.overflowing_add(1);
                *byte = new_byte;
                carry = new_carry;
            } else {
                break;
            }
        }
    }

    let byte_array: [u8; 32] = bytes
        .try_into()
        .map_err(|_| ParquetError::Conversion("Failed to convert bytes to i256".to_string()))?;
    Ok(arrow_buffer::i256::from_le_bytes(byte_array))
}

/// Build list array
fn build_list_array(values: Vec<ParquetValue>, item_field: &Arc<Field>) -> Result<ArrayRef> {
    let mut all_items = Vec::new();
    let mut offsets = Vec::with_capacity(values.len() + 1);
    let mut null_buffer_builder = arrow_buffer::BooleanBufferBuilder::new(values.len());
    offsets.push(0i32);

    for value in values {
        match value {
            ParquetValue::List(items) => {
                all_items.extend(items);
                offsets.push(all_items.len() as i32);
                null_buffer_builder.append(true);
            }
            ParquetValue::Null => {
                offsets.push(all_items.len() as i32);
                null_buffer_builder.append(false);
            }
            _ => {
                return Err(ParquetError::Conversion(format!(
                    "Expected List, got {:?}",
                    value.type_name()
                )))
            }
        }
    }

    let item_array = parquet_values_to_arrow_array(all_items, item_field)?;
    let offset_buffer = arrow_buffer::OffsetBuffer::new(offsets.into());
    let null_buffer = null_buffer_builder.finish();

    Ok(Arc::new(ListArray::new(
        item_field.clone(),
        offset_buffer,
        item_array,
        Some(null_buffer.into()),
    )))
}

/// Build map array
fn build_map_array(
    values: Vec<ParquetValue>,
    entries_field: &Arc<Field>,
    _sorted: bool,
) -> Result<ArrayRef> {
    // Extract the key and value fields from the entries struct
    let (key_field, value_field) = match entries_field.data_type() {
        DataType::Struct(fields) if fields.len() == 2 => (&fields[0], &fields[1]),
        _ => {
            return Err(ParquetError::Conversion(
                "Map entries field must be a struct with exactly 2 fields".to_string(),
            ))
        }
    };

    let mut all_keys = Vec::new();
    let mut all_values = Vec::new();
    let mut offsets = Vec::with_capacity(values.len() + 1);
    let mut null_buffer_builder = arrow_buffer::BooleanBufferBuilder::new(values.len());
    offsets.push(0i32);

    for value in values {
        match value {
            ParquetValue::Map(entries) => {
                for (k, v) in entries {
                    all_keys.push(k);
                    all_values.push(v);
                }
                offsets.push(all_keys.len() as i32);
                null_buffer_builder.append(true);
            }
            ParquetValue::Null => {
                offsets.push(all_keys.len() as i32);
                null_buffer_builder.append(false);
            }
            _ => {
                return Err(ParquetError::Conversion(format!(
                    "Expected Map, got {:?}",
                    value.type_name()
                )))
            }
        }
    }

    let key_array = parquet_values_to_arrow_array(all_keys, key_field)?;
    let value_array = parquet_values_to_arrow_array(all_values, value_field)?;

    // Create struct array for entries
    let struct_fields = match entries_field.data_type() {
        DataType::Struct(fields) => fields.clone(),
        _ => unreachable!("Map entries field must be a struct"),
    };

    let struct_array = StructArray::new(struct_fields, vec![key_array, value_array], None);

    let offset_buffer = arrow_buffer::OffsetBuffer::new(offsets.into());
    let null_buffer = null_buffer_builder.finish();

    Ok(Arc::new(MapArray::new(
        entries_field.clone(),
        offset_buffer,
        struct_array,
        Some(null_buffer.into()),
        false, // sorted
    )))
}

/// Build struct array
fn build_struct_array(
    values: Vec<ParquetValue>,
    fields: &arrow_schema::Fields,
) -> Result<ArrayRef> {
    let num_rows = values.len();
    let mut field_arrays = Vec::with_capacity(fields.len());
    let mut null_buffer_builder = arrow_buffer::BooleanBufferBuilder::new(num_rows);

    // Prepare columns for each field
    let mut field_columns: Vec<Vec<ParquetValue>> =
        vec![Vec::with_capacity(num_rows); fields.len()];

    for value in values {
        match value {
            ParquetValue::Record(map) => {
                null_buffer_builder.append(true);
                for (idx, field) in fields.iter().enumerate() {
                    let field_value = map
                        .get(field.name().as_str())
                        .cloned()
                        .unwrap_or(ParquetValue::Null);
                    field_columns[idx].push(field_value);
                }
            }
            ParquetValue::Null => {
                null_buffer_builder.append(false);
                for field_column in field_columns.iter_mut().take(fields.len()) {
                    field_column.push(ParquetValue::Null);
                }
            }
            _ => {
                return Err(ParquetError::Conversion(format!(
                    "Expected Record, got {:?}",
                    value.type_name()
                )))
            }
        }
    }

    // Build arrays for each field
    for (column, field) in field_columns.into_iter().zip(fields.iter()) {
        let array = parquet_values_to_arrow_array(column, field)?;
        field_arrays.push(array);
    }

    let null_buffer = null_buffer_builder.finish();
    Ok(Arc::new(StructArray::new(
        fields.clone(),
        field_arrays,
        Some(null_buffer.into()),
    )))
}

/// Append a single ParquetValue to an ArrayBuilder
/// This is used for incremental building in complex scenarios
pub fn append_parquet_value_to_builder(
    builder: &mut dyn ArrayBuilder,
    value: ParquetValue,
    data_type: &DataType,
) -> Result<()> {
    match data_type {
        DataType::Boolean => match value {
            ParquetValue::Boolean(b) => {
                let boolean_builder = builder
                    .as_any_mut()
                    .downcast_mut::<BooleanBuilder>()
                    .ok_or_else(|| {
                        ParquetError::Conversion("Failed to downcast to BooleanBuilder".to_string())
                    })?;
                boolean_builder.append_value(b);
            }
            ParquetValue::Null => {
                let boolean_builder = builder
                    .as_any_mut()
                    .downcast_mut::<BooleanBuilder>()
                    .ok_or_else(|| {
                        ParquetError::Conversion("Failed to downcast to BooleanBuilder".to_string())
                    })?;
                boolean_builder.append_null();
            }
            _ => {
                return Err(ParquetError::Conversion(format!(
                    "Expected Boolean, got {:?}",
                    value.type_name()
                )))
            }
        },

        // For complex types like Map and Struct, we need special handling
        DataType::Map(entries_field, _) => match value {
            ParquetValue::Map(entries) => {
                let map_builder = builder
                    .as_any_mut()
                    .downcast_mut::<MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>>()
                    .ok_or_else(|| {
                        ParquetError::Conversion("Failed to downcast to MapBuilder".to_string())
                    })?;

                if let DataType::Struct(fields) = entries_field.data_type() {
                    if fields.len() != 2 {
                        return Err(ParquetError::Conversion(
                            "Map entries struct must have exactly 2 fields".to_string(),
                        ));
                    }

                    let key_type = fields[0].data_type();
                    let value_type = fields[1].data_type();

                    for (key, val) in entries {
                        append_parquet_value_to_builder(map_builder.keys(), key, key_type)?;
                        append_parquet_value_to_builder(map_builder.values(), val, value_type)?;
                    }
                    map_builder.append(true)?;
                } else {
                    return Err(ParquetError::Conversion(
                        "Map entries field must be a struct".to_string(),
                    ));
                }
            }
            ParquetValue::Null => {
                let map_builder = builder
                    .as_any_mut()
                    .downcast_mut::<MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>>()
                    .ok_or_else(|| {
                        ParquetError::Conversion("Failed to downcast to MapBuilder".to_string())
                    })?;
                map_builder.append(false)?;
            }
            _ => {
                return Err(ParquetError::Conversion(format!(
                    "Expected Map, got {:?}",
                    value.type_name()
                )))
            }
        },

        // For other types, use the existing pattern
        _ => {
            return Err(ParquetError::Conversion(format!(
                "append_parquet_value_to_builder not implemented for type: {:?}",
                data_type
            )))
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::*;

    #[test]
    fn test_primitive_conversion_roundtrip() {
        // Test boolean
        let values = vec![
            ParquetValue::Boolean(true),
            ParquetValue::Boolean(false),
            ParquetValue::Null,
        ];
        let field = Field::new("test", DataType::Boolean, true);
        let array = parquet_values_to_arrow_array(values.clone(), &field).unwrap();

        for (i, expected) in values.iter().enumerate() {
            let actual = arrow_to_parquet_value(array.as_ref(), i).unwrap();
            assert_eq!(&actual, expected);
        }
    }

    #[test]
    fn test_integer_upcasting() {
        // Test that smaller integers can be upcast to larger ones
        let values = vec![
            ParquetValue::Int8(42),
            ParquetValue::Int16(1000),
            ParquetValue::Int32(100000),
        ];
        let field = Field::new("test", DataType::Int64, false);
        let array = parquet_values_to_arrow_array(values, &field).unwrap();

        assert_eq!(array.len(), 3);
        let int64_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int64_array.value(0), 42);
        assert_eq!(int64_array.value(1), 1000);
        assert_eq!(int64_array.value(2), 100000);
    }
}
